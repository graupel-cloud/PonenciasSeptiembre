"""Despliega la demo en Databricks: notebooks, jobs y dashboard.

Uso:
  export DATABRICKS_HOST=https://adb-....azuredatabricks.net
  export DATABRICKS_TOKEN=dapi...
  python3 deploy.py                                  # sube notebooks, crea/actualiza jobs y dashboard
  python3 deploy.py --run "Retail · Proceso diario completo" 2026-09-23   # y además lanza un job y espera
"""
import base64
import glob
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
CTX = ssl.create_default_context(cafile="/etc/ssl/cert.pem")
AQUI = os.path.dirname(os.path.abspath(__file__))

JOB_GENERAR = "Retail 1 · Generar ventas del día (cajas → landing)"
JOB_ETL = "Retail 2 · ETL diario (bronze → silver → gold → informe)"
JOB_COMPLETO = "Retail · Proceso diario completo"
OBSOLETOS = ["Retail - informe diario de ventas (400 tiendas)"]
DASHBOARD = "Retail · Informe de ventas diario"


def api(method, path, body=None, fatal=True):
    req = urllib.request.Request(
        HOST + "/api/" + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req, context=CTX))
    except urllib.error.HTTPError as e:
        msg = f"{method} {path} -> {e.code}: {e.read().decode()[:1500]}"
        if fatal:
            sys.exit(msg)
        return {"error": msg}


def subir_notebooks(carpeta):
    api("POST", "2.0/workspace/mkdirs", {"path": carpeta})
    notebooks = os.path.join(AQUI, "notebooks")
    for local in sorted(glob.glob(os.path.join(notebooks, "*.py")) + glob.glob(os.path.join(notebooks, "*.sql"))):
        nombre, ext = os.path.splitext(os.path.basename(local))
        content = base64.b64encode(open(local, "rb").read()).decode()
        api("POST", "2.0/workspace/import", {"path": f"{carpeta}/{nombre}", "format": "SOURCE",
                                             "language": "SQL" if ext == ".sql" else "PYTHON",
                                             "content": content, "overwrite": True})
        print("  notebook", nombre)
    api("POST", "2.0/workspace/delete", {"path": f"{carpeta}/02_etl_ventas_diarias"}, fatal=False)


def jobs_por_nombre():
    jobs, token = {}, ""
    while True:
        r = api("GET", f"2.1/jobs/list?limit=100&page_token={token}")
        for j in r.get("jobs", []):
            jobs[j["settings"]["name"]] = j["job_id"]
        token = r.get("next_page_token")
        if not token:
            return jobs


def upsert_job(settings, existentes):
    if settings["name"] in existentes:
        job_id = existentes[settings["name"]]
        api("POST", "2.1/jobs/reset", {"job_id": job_id, "new_settings": settings})
    else:
        job_id = api("POST", "2.1/jobs/create", settings)["job_id"]
    print(f"  job {job_id}  {settings['name']}")
    return job_id


def nb_task(key, carpeta, notebook, descripcion, depende=()):
    t = {"task_key": key, "description": descripcion,
         "notebook_task": {"notebook_path": f"{carpeta}/{notebook}"}}
    if depende:
        t["depends_on"] = [{"task_key": d} for d in depende]
    return t


def desplegar_jobs(carpeta):
    existentes = jobs_por_nombre()
    for viejo in OBSOLETOS:
        if viejo in existentes:
            api("POST", "2.1/jobs/delete", {"job_id": existentes.pop(viejo)})
            print("  borrado job obsoleto", viejo)
    fecha = [{"name": "fecha", "default": ""}]
    comun = {"max_concurrent_runs": 1, "parameters": fecha, "tags": {"demo": "retail"}}

    id_generar = upsert_job({
        **comun, "name": JOB_GENERAR,
        "description": "Simula las cajas de las 400 tiendas y deja los ficheros del día en el volume landing. "
                       "Parámetro fecha (YYYY-MM-DD); vacío = ayer.",
        "tasks": [nb_task("generar_ventas", carpeta, "01_generar_ventas",
                          "Cajas registradoras -> ficheros CSV en /Volumes/.../landing/ventas/dt=...")],
    }, existentes)

    id_etl = upsert_job({
        **comun, "name": JOB_ETL,
        "description": "¿Cuánto vendió ayer cada tienda y de qué productos? Medallion bronze -> silver -> gold "
                       "+ control de calidad + informe. Parámetro fecha (YYYY-MM-DD); vacío = ayer.",
        "tasks": [
            nb_task("bronze_ingesta", carpeta, "02_bronze_ingesta", "Ficheros de landing -> ventas_bronze"),
            nb_task("silver_limpieza", carpeta, "03_silver_limpieza", "Validación y tipado -> ventas_lineas",
                    ["bronze_ingesta"]),
            nb_task("calidad_datos", carpeta, "04_calidad_datos", "Contadores de calidad -> calidad_datos",
                    ["silver_limpieza"]),
            nb_task("gold_agregados", carpeta, "05_gold_agregados", "Ventas por tienda x producto y resúmenes",
                    ["silver_limpieza"]),
            nb_task("informe", carpeta, "06_informe", "Informe de la mañana",
                    ["gold_agregados", "calidad_datos"]),
        ],
    }, existentes)

    id_completo = upsert_job({
        **comun, "name": JOB_COMPLETO,
        "description": "Orquesta el día completo: genera las ventas y ejecuta el ETL. Programado cada mañana a las "
                       "07:00 (en pausa para la demo). Parámetro fecha (YYYY-MM-DD); vacío = ayer.",
        "schedule": {"quartz_cron_expression": "0 0 7 * * ?", "timezone_id": "Europe/Madrid",
                     "pause_status": "PAUSED"},
        "tasks": [
            {"task_key": "generar_ventas", "description": "Job: " + JOB_GENERAR,
             "run_job_task": {"job_id": id_generar, "job_parameters": {"fecha": "{{job.parameters.fecha}}"}}},
            {"task_key": "etl_diario", "description": "Job: " + JOB_ETL, "depends_on": [{"task_key": "generar_ventas"}],
             "run_job_task": {"job_id": id_etl, "job_parameters": {"fecha": "{{job.parameters.fecha}}"}}},
        ],
    }, existentes)
    return {JOB_GENERAR: id_generar, JOB_ETL: id_etl, JOB_COMPLETO: id_completo}


def desplegar_dashboard(carpeta):
    import dashboard
    warehouses = api("GET", "2.0/sql/warehouses").get("warehouses", [])
    warehouse_id = next(w["id"] for w in warehouses if w.get("enable_serverless_compute")) if warehouses else None
    cuerpo = {"display_name": DASHBOARD, "warehouse_id": warehouse_id, "parent_path": carpeta,
              "serialized_dashboard": json.dumps(dashboard.spec(), ensure_ascii=False)}
    existente = None
    token = ""
    while True:
        r = api("GET", f"2.0/lakeview/dashboards?page_size=100&page_token={token}")
        existente = existente or next((d for d in r.get("dashboards", []) if d["display_name"] == DASHBOARD), None)
        token = r.get("next_page_token")
        if not token:
            break
    if existente:
        dash_id = existente["dashboard_id"]
        cuerpo.pop("parent_path")
        api("PATCH", f"2.0/lakeview/dashboards/{dash_id}", cuerpo)
    else:
        dash_id = api("POST", "2.0/lakeview/dashboards", cuerpo)["dashboard_id"]
    api("POST", f"2.0/lakeview/dashboards/{dash_id}/published",
        {"warehouse_id": warehouse_id, "embed_credentials": True})
    print(f"  dashboard {HOST}/dashboardsv3/{dash_id}/published")
    return dash_id


def lanzar(job_id, fecha):
    run_id = api("POST", "2.1/jobs/run-now", {"job_id": job_id, "job_parameters": {"fecha": fecha}})["run_id"]
    print(f"run {run_id}  {HOST}/jobs/{job_id}/runs/{run_id}")
    t0 = time.time()
    while True:
        run = api("GET", f"2.1/jobs/runs/get?run_id={run_id}")
        if run["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            break
        time.sleep(10)
    print(run["state"].get("result_state"), f"{time.time() - t0:.0f} s")
    return run


def main():
    me = api("GET", "2.0/preview/scim/v2/Me")["userName"]
    carpeta = f"/Users/{me}/retail_demo"
    print("Notebooks ->", carpeta)
    subir_notebooks(carpeta)
    print("Jobs")
    ids = desplegar_jobs(carpeta)
    print("Dashboard")
    desplegar_dashboard(carpeta)
    if len(sys.argv) > 3 and sys.argv[1] == "--run":
        lanzar(ids[sys.argv[2]], sys.argv[3])


if __name__ == "__main__":
    main()
