package com.demo.retail;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point.
 *
 * <pre>
 *   hadoop jar retail-etl.jar generate [--date YYYY-MM-DD] [--scale 1.0]
 *   hadoop jar retail-etl.jar etl      [--date YYYY-MM-DD]
 * </pre>
 * The date defaults to yesterday (Europe/Madrid).
 */
public final class RetailMain {

  private RetailMain() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      System.exit(1);
    }
    String[] rest = Arrays.copyOfRange(args, 1, args.length);
    int rc;
    switch (args[0]) {
      case "generate":
        rc = ToolRunner.run(new Configuration(), new GenerateSales(), rest);
        break;
      case "etl":
        rc = ToolRunner.run(new Configuration(), new DailySalesETL(), rest);
        break;
      default:
        usage();
        rc = 1;
    }
    System.exit(rc);
  }

  private static void usage() {
    System.err.println("Uso:\n"
        + "  hadoop jar retail-etl.jar generate [--date YYYY-MM-DD] [--scale 1.0]\n"
        + "  hadoop jar retail-etl.jar etl      [--date YYYY-MM-DD]\n"
        + "La fecha por defecto es ayer (Europe/Madrid).");
  }

  static Map<String, String> parseOpts(String[] args) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--") && i + 1 < args.length) {
        m.put(args[i].substring(2), args[++i]);
      }
    }
    return m;
  }

  static LocalDate dateOpt(Map<String, String> opts) {
    String d = opts.get("date");
    return d != null ? LocalDate.parse(d) : LocalDate.now(ZoneId.of("Europe/Madrid")).minusDays(1);
  }
}
