package demo.reputacion;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Lógica de alerta por rule_tag: "NEGATIVE_THRESHOLD negativos NUEVOS en los últimos WINDOW_MINUTES".
 *
 * Por qué se evalúa al llegar cada post y no con ventanas de event time: con tan pocos posts
 * (unos pocos por hora en la demo) las marcas de agua no avanzarían, las ventanas no se cerrarían
 * y la alerta tardaría mucho en salir o no saldría nunca. Aquí cada post se evalúa en cuanto llega,
 * con una ventana deslizante guardada en ListState y medida con published_at.
 *
 * Reglas:
 *  - Ventana: se conservan los posts con published_at >= (published_at más reciente visto - WINDOW_MINUTES).
 *  - Un post_id repetido (reconexión, reenvío) se ignora.
 *  - Solo cuentan los "negative"; "unknown" no cuenta.
 *  - Un negativo que ya formó parte de una alerta no vuelve a contar: para la siguiente alerta
 *    hacen falta NEGATIVE_THRESHOLD negativos nuevos.
 *  - Entre dos alertas de la misma clave pasan al menos COOLDOWN_MINUTES (reloj del servidor).
 *    Si se cumplen las condiciones durante la espera, se programa un temporizador y la alerta sale
 *    al terminar la espera, sin necesidad de que llegue otro post.
 *
 * El alert_id se deriva de rule_tag + post_id de los negativos que la disparan, así que si Flink
 * reprocesa posts tras restaurar un checkpoint, la alerta repetida tiene el mismo alert_id y el
 * notificador no manda un segundo correo.
 */
public class NegativeBurstAlertFunction extends KeyedProcessFunction<String, Post, KeyedJson> {
    private static final Logger LOG = LoggerFactory.getLogger(NegativeBurstAlertFunction.class);

    private final int threshold;
    private final int windowMinutes;
    private final int cooldownMinutes;
    private final String brandName;

    private transient ListState<WindowEntry> window;
    private transient ValueState<Long> maxPublishedMs;
    private transient ValueState<Long> lastAlertAtMs;
    private transient ValueState<Long> pendingTimerMs;

    // Métricas visibles en la interfaz de Flink (caja alert-logic > Metrics)
    private transient Counter alertsEmitted;
    private transient Counter alertsDeferred;
    private transient Counter duplicatesIgnored;

    public NegativeBurstAlertFunction(int threshold, int windowMinutes, int cooldownMinutes, String brandName) {
        if (threshold < 1 || windowMinutes < 1 || cooldownMinutes < 0) {
            throw new IllegalArgumentException("Parámetros de alerta no válidos");
        }
        this.threshold = threshold;
        this.windowMinutes = windowMinutes;
        this.cooldownMinutes = cooldownMinutes;
        this.brandName = brandName;
    }

    @Override
    public void open(OpenContext openContext) {
        // TTL para que el estado no crezca sin límite: al menos 3 h y siempre más que ventana + espera.
        long ttlMinutes = Math.max(180, windowMinutes + cooldownMinutes + 60);
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofMinutes(ttlMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ListStateDescriptor<WindowEntry> windowDesc = new ListStateDescriptor<>("window", WindowEntry.class);
        windowDesc.enableTimeToLive(ttl);
        window = getRuntimeContext().getListState(windowDesc);

        ValueStateDescriptor<Long> maxDesc = new ValueStateDescriptor<>("max-published-ms", Types.LONG);
        maxDesc.enableTimeToLive(ttl);
        maxPublishedMs = getRuntimeContext().getState(maxDesc);

        ValueStateDescriptor<Long> lastDesc = new ValueStateDescriptor<>("last-alert-at-ms", Types.LONG);
        lastDesc.enableTimeToLive(ttl);
        lastAlertAtMs = getRuntimeContext().getState(lastDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("pending-timer-ms", Types.LONG);
        timerDesc.enableTimeToLive(ttl);
        pendingTimerMs = getRuntimeContext().getState(timerDesc);

        alertsEmitted = getRuntimeContext().getMetricGroup().counter("alerts_emitted");
        alertsDeferred = getRuntimeContext().getMetricGroup().counter("alerts_deferred");
        duplicatesIgnored = getRuntimeContext().getMetricGroup().counter("duplicates_ignored");
    }

    @Override
    public void processElement(Post post, Context ctx, Collector<KeyedJson> out) throws Exception {
        List<WindowEntry> entries = load();
        for (WindowEntry e : entries) {
            if (e.post.postId.equals(post.postId)) {
                LOG.info("event=duplicate_ignored post_id={} rule_tag={}", post.postId, ctx.getCurrentKey());
                duplicatesIgnored.inc();
                return;
            }
        }
        long pub = post.publishedAtMillis();
        entries.add(new WindowEntry(post, pub));

        Long prevMax = maxPublishedMs.value();
        long max = prevMax == null ? pub : Math.max(prevMax, pub);
        maxPublishedMs.update(max);

        evaluate(ctx.getCurrentKey(), entries, max, ctx.timerService().currentProcessingTime(), ctx.timerService(), out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<KeyedJson> out) throws Exception {
        Long pending = pendingTimerMs.value();
        if (pending == null || pending != timestamp) {
            return;
        }
        pendingTimerMs.clear();
        Long max = maxPublishedMs.value();
        if (max == null) {
            return;
        }
        LOG.info("event=cooldown_finished rule_tag={}", ctx.getCurrentKey());
        // El temporizador puede dispararse unos ms antes de que System.currentTimeMillis() llegue a su
        // timestamp (el planificador usa el reloj monótono). Se toma el timestamp del temporizador como
        // "ahora" para no volver a programar el mismo temporizador en bucle.
        long now = Math.max(timestamp, ctx.timerService().currentProcessingTime());
        evaluate(ctx.getCurrentKey(), load(), max, now, ctx.timerService(), out);
    }

    private List<WindowEntry> load() throws Exception {
        List<WindowEntry> entries = new ArrayList<>();
        Iterable<WindowEntry> it = window.get();
        if (it != null) {
            for (WindowEntry e : it) {
                entries.add(e);
            }
        }
        return entries;
    }

    private void evaluate(String key, List<WindowEntry> entries, long maxPub, long now, TimerService timers,
                          Collector<KeyedJson> out) throws Exception {
        long cutoff = maxPub - Duration.ofMinutes(windowMinutes).toMillis();
        List<WindowEntry> kept = new ArrayList<>();
        for (WindowEntry e : entries) {
            if (e.publishedMs >= cutoff) {
                kept.add(e);
            } else {
                LOG.info("event=left_window post_id={} rule_tag={}", e.post.postId, key);
            }
        }

        List<WindowEntry> fresh = kept.stream()
                .filter(e -> e.isNegative() && !e.alerted)
                .collect(Collectors.toList());
        long negativesInWindow = kept.stream().filter(WindowEntry::isNegative).count();

        LOG.info("event=window_evaluated rule_tag={} posts_in_window={} negatives_in_window={} new_negatives={} threshold={}",
                key, kept.size(), negativesInWindow, fresh.size(), threshold);

        if (fresh.size() >= threshold) {
            Long last = lastAlertAtMs.value();
            long cooldownMs = Duration.ofMinutes(cooldownMinutes).toMillis();
            if (last == null || now - last >= cooldownMs) {
                out.collect(buildAlert(key, kept, fresh, negativesInWindow));
                alertsEmitted.inc();
                for (WindowEntry f : fresh) {
                    f.alerted = true;
                }
                lastAlertAtMs.update(now);
                Long pending = pendingTimerMs.value();
                if (pending != null) {
                    timers.deleteProcessingTimeTimer(pending);
                    pendingTimerMs.clear();
                }
            } else if (pendingTimerMs.value() == null) {
                long fireAt = Math.max(last + cooldownMs, now + 1);
                timers.registerProcessingTimeTimer(fireAt);
                pendingTimerMs.update(fireAt);
                alertsDeferred.inc();
                LOG.info("event=alert_deferred rule_tag={} new_negatives={} wait_ms={}",
                        key, fresh.size(), fireAt - now);
            }
        }

        window.update(kept);
    }

    private KeyedJson buildAlert(String key, List<WindowEntry> kept, List<WindowEntry> fresh,
                                 long negativesInWindow) throws Exception {
        String ids = fresh.stream().map(e -> e.post.postId).sorted().collect(Collectors.joining(","));
        String alertId = UUID.nameUUIDFromBytes(("alert|" + key + "|" + ids).getBytes(StandardCharsets.UTF_8)).toString();
        WindowEntry trigger = fresh.get(fresh.size() - 1);
        long firstMs = fresh.stream().mapToLong(e -> e.publishedMs).min().orElse(trigger.publishedMs);

        ObjectNode a = Json.MAPPER.createObjectNode();
        a.put("alert_id", alertId);
        a.put("rule_tag", key);
        a.put("brand", brandName);
        a.put("window_minutes", windowMinutes);
        a.put("negative_threshold", threshold);
        a.put("cooldown_minutes", cooldownMinutes);
        a.put("negatives_in_window", negativesInWindow);
        a.put("new_negatives", fresh.size());
        ObjectNode counts = a.putObject("counts_in_window");
        for (String s : List.of(Sentiments.POSITIVE, Sentiments.NEGATIVE, Sentiments.NEUTRAL, Sentiments.UNKNOWN)) {
            counts.put(s, kept.stream().filter(e -> s.equals(e.post.sentiment)).count());
        }
        ArrayNode posts = a.putArray("negative_posts");
        for (WindowEntry e : fresh) {
            posts.add(Json.MAPPER.valueToTree(e.post));
        }
        a.put("first_published_at", Instant.ofEpochMilli(firstMs).toString());
        a.put("triggered_by_post_id", trigger.post.postId);
        a.put("alert_at", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

        LOG.info("event=alert_emitted alert_id={} rule_tag={} triggered_by_post_id={} new_negatives={} negative_post_ids={}",
                alertId, key, trigger.post.postId, fresh.size(), ids);
        return new KeyedJson(alertId, Json.MAPPER.writeValueAsString(a));
    }
}
