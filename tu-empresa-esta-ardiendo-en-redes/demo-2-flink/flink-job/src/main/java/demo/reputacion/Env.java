package demo.reputacion;

public final class Env {
    private Env() {}

    public static String get(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public static int getInt(String name, int def) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Variable " + name + " no es un entero: " + v, e);
        }
    }
}
