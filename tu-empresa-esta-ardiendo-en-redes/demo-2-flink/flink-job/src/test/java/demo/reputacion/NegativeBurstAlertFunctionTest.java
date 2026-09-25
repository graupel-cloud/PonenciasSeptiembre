package demo.reputacion;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NegativeBurstAlertFunctionTest {
    private static final Instant T0 = Instant.parse("2026-10-06T12:00:00Z");
    private static final long MIN = 60_000L;

    private KeyedOneInputStreamOperatorTestHarness<String, Post, KeyedJson> h;
    private int ids = 0;

    @BeforeEach
    void setUp() throws Exception {
        h = newHarness();
        h.open();
        h.setProcessingTime(1_000_000L);
    }

    @AfterEach
    void tearDown() throws Exception {
        h.close();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Post, KeyedJson> newHarness() throws Exception {
        // umbral 2, ventana 60 min, espera entre alertas 5 min
        NegativeBurstAlertFunction fn = new NegativeBurstAlertFunction(2, 60, 5, "Panadería Demo");
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(fn), p -> p.ruleTag, Types.STRING);
    }

    private Post post(String sentiment, int minuteOffset) {
        return post(String.valueOf(1790000000000000000L + (++ids)), sentiment, minuteOffset);
    }

    private static Post post(String id, String sentiment, int minuteOffset) {
        Post p = new Post();
        p.postId = id;
        p.text = "texto " + id;
        p.authorUsername = "cuenta_demo";
        p.url = "https://x.com/cuenta_demo/status/" + id;
        p.publishedAt = T0.plusSeconds(minuteOffset * 60L).toString();
        p.ingestedAt = p.publishedAt;
        p.ruleTag = "demo";
        p.source = "x";
        p.sentiment = sentiment;
        p.classifiedAt = p.publishedAt;
        p.model = "test";
        return p;
    }

    private void send(Post p) throws Exception {
        h.processElement(p, 0L);
    }

    private List<JsonNode> alerts() throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (KeyedJson k : h.extractOutputValues()) {
            out.add(Json.MAPPER.readTree(k.json));
        }
        return out;
    }

    private void advanceMinutes(int minutes) throws Exception {
        h.setProcessingTime(h.getProcessingTime() + minutes * MIN);
    }

    @Test
    void unNegativoNoAlerta() throws Exception {
        send(post("negative", 0));
        assertEquals(0, alerts().size());
    }

    @Test
    void dosNegativosEn60MinutosAlertan() throws Exception {
        Post a = post("negative", 0);
        Post b = post("negative", 8);
        send(a);
        send(b);
        List<JsonNode> out = alerts();
        assertEquals(1, out.size());
        JsonNode alert = out.get(0);
        assertEquals("demo", alert.get("rule_tag").asText());
        assertEquals(2, alert.get("negatives_in_window").asInt());
        assertEquals(2, alert.get("negative_posts").size());
        assertEquals(b.postId, alert.get("triggered_by_post_id").asText());
        assertEquals(a.publishedAt, alert.get("first_published_at").asText());
        assertEquals(2, alert.get("counts_in_window").get("negative").asInt());
    }

    @Test
    void dosNegativosSeparados61MinutosNoAlertan() throws Exception {
        send(post("negative", 0));
        send(post("negative", 61));
        assertEquals(0, alerts().size());
    }

    @Test
    void negativoLlegadoTardeFueraDeVentanaNoCuenta() throws Exception {
        send(post("negative", 61));
        send(post("negative", 0));
        assertEquals(0, alerts().size());
    }

    @Test
    void duplicadoNoCuenta() throws Exception {
        Post a = post("negative", 0);
        send(a);
        send(a.copy());
        assertEquals(0, alerts().size());
    }

    @Test
    void unknownNoCuenta() throws Exception {
        send(post("negative", 0));
        send(post("unknown", 1));
        send(post("unknown", 2));
        send(post("positive", 3));
        send(post("neutral", 4));
        assertEquals(0, alerts().size());
    }

    @Test
    void tercerNegativoTrasAlertaNoGeneraOtra() throws Exception {
        send(post("negative", 0));
        send(post("negative", 8));
        advanceMinutes(30); // incluso pasada la espera, un solo negativo nuevo no basta
        send(post("negative", 23));
        assertEquals(1, alerts().size());
    }

    @Test
    void dosNegativosNuevosTrasLaEsperaGeneranSegundaAlerta() throws Exception {
        send(post("negative", 0));
        send(post("negative", 8));
        advanceMinutes(6);
        send(post("negative", 14));
        send(post("negative", 15));
        List<JsonNode> out = alerts();
        assertEquals(2, out.size());
        assertEquals(2, out.get(1).get("new_negatives").asInt());
        assertEquals(4, out.get(1).get("negatives_in_window").asInt());
        assertTrue(!out.get(0).get("alert_id").asText().equals(out.get(1).get("alert_id").asText()));
    }

    @Test
    void laEsperaRetrasaLaSegundaAlertaHastaQueTermina() throws Exception {
        send(post("negative", 0));
        send(post("negative", 1));
        assertEquals(1, alerts().size());
        advanceMinutes(1);
        send(post("negative", 2));
        send(post("negative", 3));
        assertEquals(1, alerts().size(), "durante la espera de 5 min no sale otra alerta");
        advanceMinutes(3);
        assertEquals(1, alerts().size());
        advanceMinutes(1); // se cumplen 5 min desde la primera alerta: salta el temporizador
        assertEquals(2, alerts().size());
        advanceMinutes(30);
        assertEquals(2, alerts().size(), "no hay más negativos nuevos");
    }

    @Test
    void clavesDistintasSonIndependientes() throws Exception {
        Post a = post("negative", 0);
        Post b = post("negative", 1);
        b.ruleTag = "otra";
        send(a);
        send(b);
        assertEquals(0, alerts().size());
    }

    @Test
    void alertIdEsDeterminista() throws Exception {
        send(post("1", "negative", 0));
        send(post("2", "negative", 1));
        String first = alerts().get(0).get("alert_id").asText();

        h.close();
        h = newHarness();
        h.open();
        h.setProcessingTime(1_000_000L);
        send(post("1", "negative", 0));
        send(post("2", "negative", 1));
        assertEquals(first, alerts().get(0).get("alert_id").asText());
    }

    @Test
    void trasRestaurarCheckpointNoSeDuplicaLaAlertaYSeConservaElEstado() throws Exception {
        send(post("1", "negative", 0));
        send(post("2", "negative", 8));
        assertEquals(1, alerts().size());
        OperatorSubtaskState snapshot = h.snapshot(1L, 1L);
        long now = h.getProcessingTime();
        h.close();

        h = newHarness();
        h.setup();
        h.initializeState(snapshot);
        h.open();
        h.setProcessingTime(now + MIN);
        // reenvío por reconexión de los mismos posts y un tercer negativo: nada
        send(post("1", "negative", 0));
        send(post("2", "negative", 8));
        send(post("3", "negative", 23));
        assertEquals(0, alerts().size());
    }

    @Test
    void temporizadorPendienteSobreviveAlCheckpoint() throws Exception {
        send(post("1", "negative", 0));
        send(post("2", "negative", 1));
        advanceMinutes(1);
        send(post("3", "negative", 2));
        send(post("4", "negative", 3));
        assertEquals(1, alerts().size());
        OperatorSubtaskState snapshot = h.snapshot(1L, 1L);
        long now = h.getProcessingTime();
        h.close();

        h = newHarness();
        h.setup();
        h.initializeState(snapshot);
        h.open();
        h.setProcessingTime(now + 5 * MIN);
        List<JsonNode> out = alerts();
        assertEquals(1, out.size());
        assertEquals(2, out.get(0).get("new_negatives").asInt());
    }
}
