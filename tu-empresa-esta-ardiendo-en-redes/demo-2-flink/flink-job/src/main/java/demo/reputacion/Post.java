package demo.reputacion;

import java.time.Instant;

/**
 * Post de X tal y como viaja por x.posts.raw y x.posts.scored.
 * POJO de Flink (clase pública, constructor vacío y campos públicos) y
 * serializado con Jackson en snake_case (postId -> post_id).
 */
public class Post {
    public String postId;
    public String text;
    public String authorUsername;
    public String url;
    public String publishedAt;
    public String ingestedAt;
    public String ruleTag;
    public String source;

    // Solo en x.posts.scored
    public String sentiment;
    public String classifiedAt;
    public String model;

    public Post() {}

    public long publishedAtMillis() {
        return Instant.parse(publishedAt).toEpochMilli();
    }

    public Post copy() {
        Post p = new Post();
        p.postId = postId;
        p.text = text;
        p.authorUsername = authorUsername;
        p.url = url;
        p.publishedAt = publishedAt;
        p.ingestedAt = ingestedAt;
        p.ruleTag = ruleTag;
        p.source = source;
        p.sentiment = sentiment;
        p.classifiedAt = classifiedAt;
        p.model = model;
        return p;
    }
}
