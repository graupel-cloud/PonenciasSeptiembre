package demo.reputacion;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Convierte el JSON de x.posts.raw en Post. Los mensajes inválidos se descartan con un log, sin tirar el job. */
public class ParsePostFunction extends ProcessFunction<String, Post> {
    private static final Logger LOG = LoggerFactory.getLogger(ParsePostFunction.class);

    @Override
    public void processElement(String value, Context ctx, Collector<Post> out) {
        Post p;
        try {
            p = Json.MAPPER.readValue(value, Post.class);
        } catch (Exception e) {
            LOG.warn("event=raw_invalid_json error=\"{}\"", e.getClass().getSimpleName());
            return;
        }
        if (p.postId == null || p.postId.isBlank() || p.ruleTag == null || p.ruleTag.isBlank()) {
            LOG.warn("event=raw_missing_fields post_id={}", p.postId);
            return;
        }
        try {
            Instant.parse(p.publishedAt);
        } catch (Exception e) {
            LOG.warn("event=raw_invalid_published_at post_id={} published_at={}", p.postId, p.publishedAt);
            return;
        }
        if (p.text == null) {
            p.text = "";
        }
        LOG.info("event=raw_received post_id={} rule_tag={} published_at={}", p.postId, p.ruleTag, p.publishedAt);
        out.collect(p);
    }
}
