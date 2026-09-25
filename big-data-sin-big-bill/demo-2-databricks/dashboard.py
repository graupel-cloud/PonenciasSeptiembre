"""Definición del dashboard AI/BI "Retail · Informe de ventas diario" (lo usa deploy.py)."""

S = "databrick_graupel_demo.retail_demo"
ULTIMO = "(SELECT max(dt) FROM {t})"

DATASETS = {
    "kpi": f"""SELECT dt, date_format(dt, 'dd/MM/yyyy') AS fecha, round(sum(ventas_netas), 2) AS ventas,
  sum(tickets) AS tickets, sum(unidades) AS unidades, round(sum(ventas_netas) / sum(tickets), 2) AS ticket_medio,
  count(*) AS tiendas
FROM {S}.resumen_tienda WHERE dt = {ULTIMO.format(t=S + '.resumen_tienda')} GROUP BY dt""",
    "top_tiendas": f"""SELECT tienda_id || ' · ' || tienda AS tienda, region, ventas_netas AS ventas
FROM {S}.resumen_tienda WHERE dt = {ULTIMO.format(t=S + '.resumen_tienda')}
ORDER BY ventas_netas DESC LIMIT 15""",
    "tiendas": f"""SELECT tienda_id, tienda, ciudad, region, formato, ventas_netas, tickets, ticket_medio, top3_productos
FROM {S}.resumen_tienda WHERE dt = {ULTIMO.format(t=S + '.resumen_tienda')}""",
    "region": f"""SELECT region, ventas_netas AS ventas, tickets, tiendas
FROM {S}.resumen_region WHERE dt = {ULTIMO.format(t=S + '.resumen_region')}""",
    "categoria": f"""SELECT categoria, ventas_netas AS ventas, unidades
FROM {S}.resumen_categoria WHERE dt = {ULTIMO.format(t=S + '.resumen_categoria')}""",
    "calidad": f"""SELECT metrica, valor
FROM {S}.calidad_datos WHERE dt = {ULTIMO.format(t=S + '.calidad_datos')}""",
    "tienda_producto": f"""SELECT tienda_id || ' · ' || tienda AS tienda, producto, categoria, unidades, tickets,
  ventas_netas AS ventas
FROM {S}.ventas_tienda_producto WHERE dt = {ULTIMO.format(t=S + '.ventas_tienda_producto')}""",
    "historico": f"""SELECT dt, round(sum(ventas_netas), 2) AS ventas, sum(tickets) AS tickets,
  round(sum(ventas_netas) / sum(tickets), 2) AS ticket_medio
FROM {S}.resumen_tienda GROUP BY dt""",
    "historico_region": f"""SELECT dt, region, ventas_netas AS ventas FROM {S}.resumen_region""",
}


def _q(dataset, fields, disaggregated=False, name="main_query"):
    return {"name": name, "query": {"datasetName": dataset, "disaggregated": disaggregated,
                                    "fields": [{"name": n, "expression": e} for n, e in fields]}}


def _frame(title, desc=None):
    f = {"showTitle": True, "title": title}
    if desc:
        f["showDescription"] = True
        f["description"] = desc
    return f


def counter(name, dataset, expr, title):
    return {"name": name, "queries": [_q(dataset, [("valor", expr)])],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": "valor", "displayName": title}},
                     "frame": _frame(title)}}


def bar(name, dataset, cat, val, title, desc=None, cat_title=None, val_title="Ventas netas (€)", horizontal=False):
    cat_enc = {"fieldName": cat, "scale": {"type": "categorical", "sort": {"by": "y-reversed" if not horizontal
                                                                                  else "x-reversed"}},
               "displayName": cat_title or cat}
    val_enc = {"fieldName": "valor", "scale": {"type": "quantitative"}, "displayName": val_title}
    enc = {"x": val_enc, "y": cat_enc} if horizontal else {"x": cat_enc, "y": val_enc}
    return {"name": name, "queries": [_q(dataset, [(cat, f"`{cat}`"), ("valor", f"SUM(`{val}`)")])],
            "spec": {"version": 3, "widgetType": "bar", "encodings": enc, "frame": _frame(title, desc)}}


def pie(name, dataset, cat, val, title):
    return {"name": name, "queries": [_q(dataset, [(cat, f"`{cat}`"), ("valor", f"SUM(`{val}`)")])],
            "spec": {"version": 3, "widgetType": "pie",
                     "encodings": {"angle": {"fieldName": "valor", "scale": {"type": "quantitative"},
                                             "displayName": "Ventas netas (€)"},
                                   "color": {"fieldName": cat, "scale": {"type": "categorical"}, "displayName": cat}},
                     "frame": _frame(title)}}


def line(name, dataset, x, val, title, val_title):
    return {"name": name, "queries": [_q(dataset, [(x, f"`{x}`"), ("valor", f"SUM(`{val}`)")])],
            "spec": {"version": 3, "widgetType": "line",
                     "encodings": {"x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": "Día"},
                                   "y": {"fieldName": "valor", "scale": {"type": "quantitative"},
                                         "displayName": val_title}},
                     "frame": _frame(title)}}


def table(name, dataset, cols, title):
    return {"name": name, "queries": [_q(dataset, [(c, f"`{c}`") for c, _ in cols], disaggregated=True)],
            "spec": {"version": 1, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": c, "displayName": t} for c, t in cols]},
                     "frame": _frame(title)}}


def filtro(name, datasets, field, title):
    queries = [_q(ds, [(field, f"`{field}`")], name=f"f_{ds}") for ds in datasets]
    return {"name": name, "queries": queries,
            "spec": {"version": 2, "widgetType": "filter-single-select",
                     "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": f"f_{ds}"}
                                              for ds in datasets]},
                     "frame": _frame(title)}}


def texto(name, lines):
    return {"name": name, "multilineTextboxSpec": {"lines": lines}}


def at(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def spec():
    ayer = [
        at(texto("titulo", ["# ¿Cuánto vendió ayer cada tienda y de qué productos?\n",
                            "Último día cargado por el job *Retail 2 · ETL diario*. "
                            "400 tiendas · datos sintéticos."]), 0, 0, 6, 2),
        at(counter("k_fecha", "kpi", "MAX(`fecha`)", "Día"), 0, 2, 1, 3),
        at(counter("k_ventas", "kpi", "SUM(`ventas`)", "Ventas netas (€)"), 1, 2, 2, 3),
        at(counter("k_tickets", "kpi", "SUM(`tickets`)", "Tickets"), 3, 2, 1, 3),
        at(counter("k_ticket_medio", "kpi", "SUM(`ticket_medio`)", "Ticket medio (€)"), 4, 2, 1, 3),
        at(counter("k_unidades", "kpi", "SUM(`unidades`)", "Unidades"), 5, 2, 1, 3),
        at(bar("b_top", "top_tiendas", "tienda", "ventas", "Top 15 tiendas", cat_title="Tienda", horizontal=True),
           0, 5, 3, 8),
        at(bar("b_region", "region", "region", "ventas", "Ventas por región", cat_title="Región"), 3, 5, 3, 8),
        at(pie("p_cat", "categoria", "categoria", "ventas", "Ventas por categoría"), 0, 13, 3, 7),
        at(bar("b_calidad", "calidad", "metrica", "valor", "Calidad de datos (líneas)", cat_title="Métrica",
               val_title="Líneas", horizontal=True), 3, 13, 3, 7),
        at(table("t_tiendas", "tiendas",
                 [("tienda_id", "Id"), ("tienda", "Tienda"), ("region", "Región"), ("formato", "Formato"),
                  ("ventas_netas", "Ventas netas (€)"), ("tickets", "Tickets"), ("ticket_medio", "Ticket medio"),
                  ("top3_productos", "Top 3 productos")], "Las 400 tiendas"), 0, 20, 6, 10),
    ]
    detalle = [
        at(texto("titulo2", ["# ¿Qué vendió ayer esta tienda?\n",
                             "Elige una tienda (sin selección: toda la cadena)."]), 0, 0, 4, 2),
        at(filtro("f_tienda", ["tienda_producto"], "tienda", "Tienda"), 4, 0, 2, 2),
        at(counter("k2_ventas", "tienda_producto", "SUM(`ventas`)", "Ventas netas (€)"), 0, 2, 2, 3),
        at(counter("k2_unidades", "tienda_producto", "SUM(`unidades`)", "Unidades"), 2, 2, 2, 3),
        at(counter("k2_refs", "tienda_producto", "COUNT(DISTINCT `producto`)", "Referencias vendidas"), 4, 2, 2, 3),
        at(bar("b2_prod", "tienda_producto", "producto", "ventas", "Top productos", cat_title="Producto",
               horizontal=True), 0, 5, 3, 9),
        at(pie("p2_cat", "tienda_producto", "categoria", "ventas", "Ventas por categoría"), 3, 5, 3, 9),
        at(table("t2_det", "tienda_producto",
                 [("tienda", "Tienda"), ("producto", "Producto"), ("categoria", "Categoría"), ("unidades", "Unidades"),
                  ("tickets", "Tickets"), ("ventas", "Ventas netas (€)")], "Detalle tienda x producto"), 0, 14, 6, 9),
    ]
    historico = [
        at(texto("titulo3", ["# Evolución diaria\n", "Un punto por cada día procesado por el ETL."]), 0, 0, 6, 2),
        at(bar("b3_dias", "historico", "dt", "ventas", "Ventas netas por día", cat_title="Día"), 0, 2, 3, 7),
        at(line("l3_ticket", "historico", "dt", "ticket_medio", "Ticket medio por día", "Ticket medio (€)"), 3, 2, 3, 7),
        at(table("t3_region", "historico_region", [("dt", "Día"), ("region", "Región"), ("ventas", "Ventas netas (€)")],
                 "Ventas por día y región"), 0, 9, 6, 8),
    ]
    return {
        "datasets": [{"name": k, "displayName": k, "queryLines": [v]} for k, v in DATASETS.items()],
        "pages": [
            {"name": "ayer", "displayName": "Ayer", "pageType": "PAGE_TYPE_CANVAS", "layout": ayer},
            {"name": "detalle", "displayName": "Detalle por tienda", "pageType": "PAGE_TYPE_CANVAS", "layout": detalle},
            {"name": "historico", "displayName": "Histórico", "pageType": "PAGE_TYPE_CANVAS", "layout": historico},
        ],
    }
