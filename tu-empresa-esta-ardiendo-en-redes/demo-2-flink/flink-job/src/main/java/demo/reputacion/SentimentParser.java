package demo.reputacion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Valida la respuesta de /api/chat de Ollama y extrae el sentimiento.
 * Cualquier cosa que no sea exactamente positive/negative/neutral devuelve "unknown".
 *
 * Nota: en Ollama Cloud algunos modelos (p. ej. deepseek-v4.1-flash) no aplican el esquema de
 * "format" y pueden envolver el JSON en ```json ... ```. Se admite ese envoltorio, pero el
 * contenido se valida igual de estricto.
 */
public final class SentimentParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern FENCE = Pattern.compile("^```[a-zA-Z]*\\s*|\\s*```$");

    private SentimentParser() {}

    /** @param chatResponseBody cuerpo completo de la respuesta HTTP de /api/chat (stream=false). */
    public static String fromChatResponse(String chatResponseBody) {
        try {
            JsonNode root = MAPPER.readTree(chatResponseBody);
            JsonNode content = root.path("message").path("content");
            if (!content.isTextual()) {
                return Sentiments.UNKNOWN;
            }
            return fromContent(content.asText());
        } catch (Exception e) {
            return Sentiments.UNKNOWN;
        }
    }

    /** @param content texto devuelto por el modelo, que debería ser {"sentiment": "..."}. */
    public static String fromContent(String content) {
        if (content == null) {
            return Sentiments.UNKNOWN;
        }
        String s = FENCE.matcher(content.trim()).replaceAll("").trim();
        try {
            JsonNode node = MAPPER.readTree(s);
            if (node == null || !node.isObject()) {
                return Sentiments.UNKNOWN;
            }
            JsonNode v = node.get("sentiment");
            if (v == null || !v.isTextual()) {
                return Sentiments.UNKNOWN;
            }
            String sentiment = v.asText().trim().toLowerCase(Locale.ROOT);
            return Sentiments.VALID.contains(sentiment) ? sentiment : Sentiments.UNKNOWN;
        } catch (Exception e) {
            return Sentiments.UNKNOWN;
        }
    }
}
