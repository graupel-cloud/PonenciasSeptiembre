package demo.reputacion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Clasifica cada post llamando a Ollama Cloud (POST {OLLAMA_BASE_URL}/chat) sin bloquear el hilo de Flink.
 * Timeout, error HTTP o respuesta no válida => sentiment = "unknown"; el job nunca falla por Ollama.
 *
 * La API key se lee del entorno en open() y no forma parte de la función serializada en el grafo del job.
 */
public class OllamaClassifyFunction extends RichAsyncFunction<Post, Post> {
    private static final Logger LOG = LoggerFactory.getLogger(OllamaClassifyFunction.class);
    private static final int MAX_TEXT_CHARS = 1000;
    private static final Pattern DELIMITERS = Pattern.compile("(?i)</?\\s*comentario\\s*>");

    private final String baseUrl;
    private final String model;
    private final String brandName;
    private final String think;
    private final Duration requestTimeout;

    private transient HttpClient http;
    private transient String apiKey;
    private transient String promptTemplate;
    private transient ObjectMapper mapper;
    private transient Map<String, Counter> sentimentCounters;
    private transient volatile long lastLatencyMs;

    public OllamaClassifyFunction(String baseUrl, String model, String brandName, String think, Duration requestTimeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
        this.brandName = brandName;
        this.think = think;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        apiKey = Env.get("OLLAMA_API_KEY", "");
        mapper = new ObjectMapper();
        // Métricas visibles en la interfaz de Flink (caja classify-ollama > Metrics)
        sentimentCounters = new HashMap<>();
        for (String s : new String[]{Sentiments.POSITIVE, Sentiments.NEGATIVE, Sentiments.NEUTRAL, Sentiments.UNKNOWN}) {
            sentimentCounters.put(s, getRuntimeContext().getMetricGroup().counter("posts_" + s));
        }
        getRuntimeContext().getMetricGroup().gauge("ollama_last_latency_ms", (Gauge<Long>) () -> lastLatencyMs);
        try (InputStream in = getClass().getResourceAsStream("/classify_prompt.txt")) {
            if (in == null) {
                throw new IOException("No se encuentra classify_prompt.txt en el jar");
            }
            promptTemplate = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        if (apiKey.isEmpty()) {
            LOG.warn("event=ollama_config_warning msg=\"OLLAMA_API_KEY vacía: todos los posts saldrán como unknown\"");
        }
        LOG.info("event=classifier_ready model={} base_url={} api_key={}", model, baseUrl, mask(apiKey));
    }

    /** El texto del post es contenido de terceros: se recorta y se neutralizan los delimitadores. */
    static String sanitize(String text) {
        String t = text == null ? "" : text;
        if (t.length() > MAX_TEXT_CHARS) {
            t = t.substring(0, MAX_TEXT_CHARS);
        }
        return DELIMITERS.matcher(t).replaceAll("[comentario]");
    }

    String buildRequestBody(String text) throws IOException {
        String prompt = promptTemplate
                .replace("{BRAND_NAME}", brandName)
                .replace("{TEXT}", sanitize(text));
        ObjectNode body = mapper.createObjectNode();
        body.put("model", model);
        body.put("stream", false);
        if ("true".equalsIgnoreCase(think) || "false".equalsIgnoreCase(think)) {
            body.put("think", Boolean.parseBoolean(think));
        }
        body.putObject("options").put("temperature", 0);
        ObjectNode format = body.putObject("format");
        format.put("type", "object");
        ObjectNode sentiment = format.putObject("properties").putObject("sentiment");
        sentiment.put("type", "string");
        ArrayNode values = sentiment.putArray("enum");
        values.add(Sentiments.POSITIVE).add(Sentiments.NEGATIVE).add(Sentiments.NEUTRAL);
        format.putArray("required").add("sentiment");
        ArrayNode messages = body.putArray("messages");
        messages.addObject().put("role", "user").put("content", prompt);
        return mapper.writeValueAsString(body);
    }

    @Override
    public void asyncInvoke(Post in, ResultFuture<Post> resultFuture) throws Exception {
        long start = System.currentTimeMillis();
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/chat"))
                .timeout(requestTimeout)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(buildRequestBody(in.text), StandardCharsets.UTF_8))
                .build();

        http.sendAsync(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenComplete((resp, err) -> {
                    String sentiment;
                    long ms = System.currentTimeMillis() - start;
                    if (err != null) {
                        sentiment = Sentiments.UNKNOWN;
                        Throwable cause = err.getCause() != null ? err.getCause() : err;
                        LOG.warn("event=ollama_error post_id={} error=\"{}\" latency_ms={}",
                                in.postId, cause.getClass().getSimpleName(), ms);
                    } else if (resp.statusCode() != 200) {
                        sentiment = Sentiments.UNKNOWN;
                        LOG.warn("event=ollama_http_error post_id={} status={} body=\"{}\" latency_ms={}",
                                in.postId, resp.statusCode(), oneLine(resp.body(), 200), ms);
                    } else {
                        sentiment = SentimentParser.fromChatResponse(resp.body());
                        if (Sentiments.UNKNOWN.equals(sentiment)) {
                            LOG.warn("event=ollama_invalid_response post_id={} body=\"{}\" latency_ms={}",
                                    in.postId, oneLine(resp.body(), 200), ms);
                        }
                    }
                    lastLatencyMs = ms;
                    Post out = scored(in, sentiment);
                    LOG.info("event=post_classified post_id={} sentiment={} model={} latency_ms={}",
                            in.postId, sentiment, model, ms);
                    resultFuture.complete(Collections.singleton(out));
                });
    }

    @Override
    public void timeout(Post in, ResultFuture<Post> resultFuture) {
        LOG.warn("event=ollama_timeout post_id={} sentiment=unknown", in.postId);
        resultFuture.complete(Collections.singleton(scored(in, Sentiments.UNKNOWN)));
    }

    private Post scored(Post in, String sentiment) {
        Counter c = sentimentCounters == null ? null : sentimentCounters.get(sentiment);
        if (c != null) {
            c.inc();
        }
        Post out = in.copy();
        out.sentiment = sentiment;
        out.classifiedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS).toString();
        out.model = model;
        return out;
    }

    static String oneLine(String s, int max) {
        if (s == null) {
            return "";
        }
        String t = s.replaceAll("[\\r\\n\"]+", " ");
        return t.length() > max ? t.substring(0, max) + "..." : t;
    }

    static String mask(String secret) {
        if (secret == null || secret.isEmpty()) {
            return "(vacía)";
        }
        return secret.length() <= 8 ? "****" : secret.substring(0, 4) + "****";
    }
}
