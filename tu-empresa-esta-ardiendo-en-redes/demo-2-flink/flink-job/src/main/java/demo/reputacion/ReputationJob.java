package demo.reputacion;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * x.posts.raw -> clasificación con Ollama (Async I/O) -> x.posts.scored
 *                                                     -> lógica de alerta por rule_tag -> alerts
 */
public class ReputationJob {
    private static final Logger LOG = LoggerFactory.getLogger(ReputationJob.class);

    static final String TOPIC_RAW = "x.posts.raw";
    static final String TOPIC_SCORED = "x.posts.scored";
    static final String TOPIC_ALERTS = "alerts";

    public static void main(String[] args) throws Exception {
        String bootstrap = Env.get("KAFKA_BOOTSTRAP", "kafka:9092");
        String brand = Env.get("BRAND_NAME", "Marca Demo");
        int threshold = Env.getInt("NEGATIVE_THRESHOLD", 2);
        int windowMinutes = Env.getInt("WINDOW_MINUTES", 60);
        int cooldownMinutes = Env.getInt("COOLDOWN_MINUTES", 5);
        String ollamaBaseUrl = Env.get("OLLAMA_BASE_URL", "https://ollama.com/api");
        String ollamaModel = Env.get("OLLAMA_MODEL", "deepseek-v4.1-flash");
        String ollamaThink = Env.get("OLLAMA_THINK", "false");
        String startFrom = Env.get("KAFKA_START_FROM", "latest");

        LOG.info("event=job_config brand=\"{}\" threshold={} window_minutes={} cooldown_minutes={} model={} start_from={}",
                brand, threshold, windowMinutes, cooldownMinutes, ollamaModel, startFrom);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpoint cada 30 s: al reiniciar se recupera el estado (ventana, última alerta, temporizadores)
        // y los offsets de Kafka. La retención de checkpoints se configura en FLINK_PROPERTIES.
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        // Una caja por paso en la interfaz de Flink: con tan poco volumen no cuesta nada y en la demo
        // se ve cada post pasar de la fuente a la clasificación, a la salida y a la lógica de alerta.
        env.disableOperatorChaining();

        // Primer arranque: desde el offset confirmado del grupo; si no hay, el más reciente (o el más antiguo si se pide).
        OffsetResetStrategy reset = "earliest".equalsIgnoreCase(startFrom)
                ? OffsetResetStrategy.EARLIEST : OffsetResetStrategy.LATEST;
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(TOPIC_RAW)
                .setGroupId("flink-reputacion")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(reset))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Post> raw = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-x.posts.raw")
                .uid("source-raw")
                .process(new ParsePostFunction())
                .uid("parse-raw")
                .name("parse-raw");

        DataStream<Post> scored = AsyncDataStream.orderedWait(
                        raw,
                        new OllamaClassifyFunction(ollamaBaseUrl, ollamaModel, brand, ollamaThink, Duration.ofSeconds(19)),
                        20, TimeUnit.SECONDS,
                        10)
                .uid("classify-ollama")
                .name("classify-ollama");

        scored
                .map(p -> new KeyedJson(p.postId, Json.MAPPER.writeValueAsString(p)))
                .uid("scored-to-json")
                .name("scored-to-json")
                .sinkTo(sink(bootstrap, TOPIC_SCORED))
                .uid("sink-scored")
                .name("kafka-x.posts.scored");

        scored
                .keyBy(p -> p.ruleTag)
                .process(new NegativeBurstAlertFunction(threshold, windowMinutes, cooldownMinutes, brand))
                .uid("alert-logic")
                .name("alert-logic")
                .sinkTo(sink(bootstrap, TOPIC_ALERTS))
                .uid("sink-alerts")
                .name("kafka-alerts");

        env.execute("reputacion-x");
    }

    /**
     * At-least-once: cada registro sale a Kafka en cuanto se produce (sin esperar al checkpoint, que
     * añadiría hasta 30 s de retraso en el vídeo). Los duplicados tras un reinicio se resuelven aguas
     * abajo: el panel deduplica por post_id y el notificador por alert_id, que es determinista.
     */
    private static KafkaSink<KeyedJson> sink(String bootstrap, String topic) {
        return KafkaSink.<KeyedJson>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<KeyedJson>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema((KeyedJson k) -> k.key.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema((KeyedJson k) -> k.json.getBytes(StandardCharsets.UTF_8))
                        .build())
                .build();
    }
}
