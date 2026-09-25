package demo.reputacion;

/** Elemento del ListState de la ventana deslizante de la lógica de alertas. */
public class WindowEntry {
    public Post post;
    public long publishedMs;
    /** true si este negativo ya se usó en una alerta anterior (no vuelve a contar como "nuevo"). */
    public boolean alerted;

    public WindowEntry() {}

    public WindowEntry(Post post, long publishedMs) {
        this.post = post;
        this.publishedMs = publishedMs;
        this.alerted = false;
    }

    public boolean isNegative() {
        return Sentiments.NEGATIVE.equals(post.sentiment);
    }
}
