package demo.reputacion;

import java.util.Set;

public final class Sentiments {
    public static final String POSITIVE = "positive";
    public static final String NEGATIVE = "negative";
    public static final String NEUTRAL = "neutral";
    public static final String UNKNOWN = "unknown";

    public static final Set<String> VALID = Set.of(POSITIVE, NEGATIVE, NEUTRAL);

    private Sentiments() {}
}
