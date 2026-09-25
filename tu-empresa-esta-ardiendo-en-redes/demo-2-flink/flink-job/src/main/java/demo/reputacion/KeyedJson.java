package demo.reputacion;

/** Mensaje listo para Kafka: clave y valor JSON. */
public class KeyedJson {
    public String key;
    public String json;

    public KeyedJson() {}

    public KeyedJson(String key, String json) {
        this.key = key;
        this.json = json;
    }
}
