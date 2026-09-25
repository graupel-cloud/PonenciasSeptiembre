package com.demo.retail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * The morning question: how much did each store sell yesterday, and of which products?
 *
 * <ol>
 *   <li>Job 1 cleans the raw till lines of the day (corrupt records, lines from another day,
 *       unknown products, returns) and aggregates them per store x product, enriched with the
 *       store and product master data. Output: /retail/curated/daily_sales/dt=YYYY-MM-DD</li>
 *   <li>Job 2 summarises job 1 per store (with its top 3 products), per region, per category and
 *       for the whole chain. Output: /retail/reports/dt=YYYY-MM-DD</li>
 * </ol>
 * The driver then prints the morning report and stores it as informe.txt next to the summary.
 */
public class DailySalesETL extends Configured implements Tool {

  static final String Q = "Calidad de datos";

  // ---------------------------------------------------------------- job 1: clean + aggregate

  public static class CleanMapper extends Mapper<LongWritable, Text, Text, SalesAgg> {
    private Map<String, RetailData.Product> products;
    private String day;
    private final Text outKey = new Text();
    private final SalesAgg agg = new SalesAgg();

    @Override
    protected void setup(Context ctx) throws IOException {
      FileSystem fs = FileSystem.get(ctx.getConfiguration());
      products = RetailData.readProducts(fs, new Path(RetailData.PRODUCTS_FILE));
      day = ctx.getConfiguration().get(GenerateSales.CONF_DATE);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
      ctx.getCounter(Q, "01 Lineas leidas").increment(1);
      String line = value.toString();
      String[] f = line.split(";", -1);
      if (line.startsWith("#") || f.length != 9) {
        ctx.getCounter(Q, "02 Descartadas: registro corrupto").increment(1);
        return;
      }
      if (!f[1].startsWith(day)) {
        ctx.getCounter(Q, "03 Descartadas: fuera de fecha").increment(1);
        return;
      }
      if (!products.containsKey(f[4])) {
        ctx.getCounter(Q, "04 Descartadas: producto desconocido").increment(1);
        return;
      }
      int qty;
      double price;
      int discount;
      try {
        qty = Integer.parseInt(f[5]);
        price = Double.parseDouble(f[6]);
        discount = Integer.parseInt(f[7]);
      } catch (NumberFormatException e) {
        ctx.getCounter(Q, "02 Descartadas: registro corrupto").increment(1);
        return;
      }
      agg.clear();
      agg.units = qty;
      agg.lines = 1;
      agg.tickets = "1".equals(f[3]) ? 1 : 0;
      agg.gross = qty * price;
      agg.net = agg.gross * (100 - discount) / 100.0;
      if (qty < 0) {
        agg.returnedUnits = -qty;
        ctx.getCounter(Q, "06 Lineas de devolucion").increment(1);
      }
      ctx.getCounter(Q, "05 Lineas validas").increment(1);
      outKey.set(f[2] + "|" + f[4]);
      ctx.write(outKey, agg);
    }
  }

  /** Combiner: partial sums on the map side. */
  public static class SumCombiner extends Reducer<Text, SalesAgg, Text, SalesAgg> {
    private final SalesAgg sum = new SalesAgg();

    @Override
    protected void reduce(Text key, Iterable<SalesAgg> values, Context ctx) throws IOException, InterruptedException {
      sum.clear();
      for (SalesAgg v : values) {
        sum.add(v);
      }
      ctx.write(key, sum);
    }
  }

  /** Final sums, enriched with store and product master data, as a TSV line. */
  public static class EnrichReducer extends Reducer<Text, SalesAgg, NullWritable, Text> {
    private Map<String, RetailData.Store> stores;
    private Map<String, RetailData.Product> products;
    private String day;
    private final SalesAgg sum = new SalesAgg();
    private final Text out = new Text();

    @Override
    protected void setup(Context ctx) throws IOException {
      FileSystem fs = FileSystem.get(ctx.getConfiguration());
      stores = RetailData.readStores(fs, new Path(RetailData.STORES_FILE));
      products = RetailData.readProducts(fs, new Path(RetailData.PRODUCTS_FILE));
      day = ctx.getConfiguration().get(GenerateSales.CONF_DATE);
    }

    @Override
    protected void reduce(Text key, Iterable<SalesAgg> values, Context ctx) throws IOException, InterruptedException {
      sum.clear();
      for (SalesAgg v : values) {
        sum.add(v);
      }
      String[] k = key.toString().split("\\|");
      RetailData.Store s = stores.get(k[0]);
      RetailData.Product p = products.get(k[1]);
      out.set(String.join("\t", day, s.id, s.name, s.city, s.region, s.format, p.sku, p.name, p.category,
          Long.toString(sum.units), Long.toString(sum.returnedUnits), Long.toString(sum.lines),
          Long.toString(sum.tickets), fmt(sum.gross), fmt(sum.net)));
      ctx.write(NullWritable.get(), out);
    }
  }

  // ---------------------------------------------------------------- job 2: summaries

  public static class SummaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text k = new Text();
    private final Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
      // dt store_id store_name city region format sku product category units returned lines tickets gross net
      String[] f = value.toString().split("\t", -1);
      String units = f[9];
      String tickets = f[12];
      String net = f[14];
      emit(ctx, "S\t" + f[1], String.join("\t", net, units, tickets, f[6], f[7], f[2], f[3], f[4], f[5]));
      emit(ctx, "R\t" + f[4], String.join("\t", net, units, tickets));
      emit(ctx, "C\t" + f[8], String.join("\t", net, units, "0"));
      emit(ctx, "T\tCADENA", String.join("\t", net, units, tickets));
    }

    private void emit(Context ctx, String key, String value) throws IOException, InterruptedException {
      k.set(key);
      v.set(value);
      ctx.write(k, v);
    }
  }

  public static class SummaryReducer extends Reducer<Text, Text, NullWritable, Text> {
    private final Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
      String[] k = key.toString().split("\t", 2);
      double net = 0;
      long units = 0;
      long tickets = 0;
      String[] storeAttrs = null;
      List<String[]> top = new ArrayList<>();
      for (Text t : values) {
        String[] f = t.toString().split("\t", -1);
        double n = Double.parseDouble(f[0]);
        net += n;
        units += Long.parseLong(f[1]);
        tickets += Long.parseLong(f[2]);
        if (k[0].equals("S")) {
          storeAttrs = new String[] {f[5], f[6], f[7], f[8]};
          top.add(new String[] {f[4], f[0]});
          if (top.size() > 3) { // keep the 3 best products
            top.sort(Comparator.comparingDouble((String[] a) -> -Double.parseDouble(a[1])));
            top.remove(3);
          }
        }
      }
      switch (k[0]) {
        case "S":
          top.sort(Comparator.comparingDouble((String[] a) -> -Double.parseDouble(a[1])));
          StringBuilder tops = new StringBuilder();
          for (String[] p : top) {
            tops.append(tops.length() == 0 ? "" : " | ").append(p[0]).append(" (").append(p[1]).append(")");
          }
          out.set(String.join("\t", "TIENDA", k[1], storeAttrs[0], storeAttrs[1], storeAttrs[2], storeAttrs[3],
              fmt(net), Long.toString(units), Long.toString(tickets),
              fmt(tickets == 0 ? 0 : net / tickets), tops.toString()));
          break;
        case "R":
          out.set(String.join("\t", "REGION", k[1], fmt(net), Long.toString(units), Long.toString(tickets)));
          break;
        case "C":
          out.set(String.join("\t", "CATEGORIA", k[1], fmt(net), Long.toString(units)));
          break;
        default:
          out.set(String.join("\t", "TOTAL", k[1], fmt(net), Long.toString(units), Long.toString(tickets)));
      }
      ctx.write(NullWritable.get(), out);
    }
  }

  // ---------------------------------------------------------------- driver

  @Override
  public int run(String[] args) throws Exception {
    Map<String, String> opts = RetailMain.parseOpts(args);
    LocalDate date = RetailMain.dateOpt(opts);
    Configuration conf = getConf();
    conf.set(GenerateSales.CONF_DATE, date.toString());
    FileSystem fs = FileSystem.get(conf);

    Path raw = new Path(RetailData.RAW_DIR + "/dt=" + date);
    Path curated = new Path(RetailData.CURATED_DIR + "/dt=" + date);
    Path report = new Path(RetailData.REPORT_DIR + "/dt=" + date);
    if (!fs.exists(raw)) {
      System.err.println("No hay ventas en " + raw + ". Genera primero: hadoop jar retail-etl.jar generate --date "
          + date);
      return 2;
    }
    fs.delete(curated, true);
    fs.delete(report, true);
    long t0 = System.currentTimeMillis();

    Job j1 = Job.getInstance(conf, "Retail ETL 1/2 - limpieza y ventas tienda x producto " + date);
    j1.setJarByClass(DailySalesETL.class);
    j1.setMapperClass(CleanMapper.class);
    j1.setCombinerClass(SumCombiner.class);
    j1.setReducerClass(EnrichReducer.class);
    j1.setNumReduceTasks(8);
    j1.setMapOutputKeyClass(Text.class);
    j1.setMapOutputValueClass(SalesAgg.class);
    j1.setOutputKeyClass(NullWritable.class);
    j1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j1, raw);
    FileOutputFormat.setOutputPath(j1, curated);
    if (!j1.waitForCompletion(true)) {
      return 1;
    }

    Job j2 = Job.getInstance(conf, "Retail ETL 2/2 - resumen por tienda, region y categoria " + date);
    j2.setJarByClass(DailySalesETL.class);
    j2.setMapperClass(SummaryMapper.class);
    j2.setReducerClass(SummaryReducer.class);
    j2.setNumReduceTasks(1);
    j2.setMapOutputKeyClass(Text.class);
    j2.setMapOutputValueClass(Text.class);
    j2.setOutputKeyClass(NullWritable.class);
    j2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j2, curated);
    FileOutputFormat.setOutputPath(j2, new Path(report, "resumen"));
    if (!j2.waitForCompletion(true)) {
      return 1;
    }

    String text = buildReport(fs, new Path(report, "resumen"), date, j1.getCounters(),
        (System.currentTimeMillis() - t0) / 1000.0);
    System.out.println(text);
    try (Writer w = new OutputStreamWriter(fs.create(new Path(report, "informe.txt"), true), StandardCharsets.UTF_8)) {
      w.write(text);
    }
    System.out.println("Detalle tienda x producto: " + curated);
    System.out.println("Resumen e informe:         " + report);
    return 0;
  }

  // ---------------------------------------------------------------- report

  private static String buildReport(FileSystem fs, Path summary, LocalDate date, Counters c, double secs)
      throws IOException {
    List<String[]> stores = new ArrayList<>();
    List<String[]> regions = new ArrayList<>();
    List<String[]> categories = new ArrayList<>();
    String[] total = null;
    for (FileStatus st : fs.listStatus(summary, p -> p.getName().startsWith("part-"))) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(st.getPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] f = line.split("\t", -1);
          switch (f[0]) {
            case "TIENDA": stores.add(f); break;
            case "REGION": regions.add(f); break;
            case "CATEGORIA": categories.add(f); break;
            default: total = f;
          }
        }
      }
    }
    Comparator<String[]> byNet = Comparator.comparingDouble((String[] a) -> -Double.parseDouble(a[a[0].equals("TIENDA") ? 6 : 2]));
    stores.sort(byNet);
    regions.sort(byNet);
    categories.sort(byNet);

    double chainNet = Double.parseDouble(total[2]);
    long chainTickets = Long.parseLong(total[4]);
    StringBuilder sb = new StringBuilder();
    String rule = repeat('=', 96);
    sb.append('\n').append(rule).append('\n');
    sb.append(String.format(ES, "  INFORME DE VENTAS DE AYER  -  %s  (%s)%n", date,
        date.getDayOfWeek().getDisplayName(java.time.format.TextStyle.FULL, ES)));
    sb.append(rule).append('\n');
    sb.append(String.format(ES, "  Ventas netas de la cadena: %s   |  Tiendas: %d  |  Unidades: %,d%n",
        eur(chainNet), stores.size(), Long.parseLong(total[3])));
    sb.append(String.format(ES, "  Tickets: %,d   |  Ticket medio: %s   |  ETL ejecutado en %.1f s%n",
        chainTickets, eur(chainNet / Math.max(1, chainTickets)), secs));

    sb.append("\n  TOP 10 TIENDAS\n");
    sb.append(String.format(ES, "  %-5s %-34s %-18s %14s %9s  %s%n", "Id", "Tienda", "Region", "Ventas", "Tickets",
        "Productos estrella"));
    for (int i = 0; i < Math.min(10, stores.size()); i++) {
      sb.append(storeRow(stores.get(i)));
    }
    sb.append("\n  LAS 5 TIENDAS QUE MENOS VENDIERON\n");
    for (int i = Math.max(0, stores.size() - 5); i < stores.size(); i++) {
      sb.append(storeRow(stores.get(i)));
    }

    sb.append("\n  VENTAS POR REGION\n");
    for (String[] r : regions) {
      double net = Double.parseDouble(r[2]);
      sb.append(String.format(ES, "  %-20s %16s %6.1f%%  %s%n", r[1], eur(net), 100 * net / chainNet,
          bar(net / Double.parseDouble(regions.get(0)[2]))));
    }
    sb.append("\n  VENTAS POR CATEGORIA\n");
    for (String[] r : categories) {
      double net = Double.parseDouble(r[2]);
      sb.append(String.format(ES, "  %-20s %16s %6.1f%%  %,12d uds%n", r[1], eur(net), 100 * net / chainNet,
          Long.parseLong(r[3])));
    }

    sb.append("\n  CALIDAD DE DATOS (contadores del job 1)\n");
    for (org.apache.hadoop.mapreduce.Counter k : c.getGroup(Q)) {
      sb.append(String.format(ES, "  %-40s %,14d%n", k.getDisplayName().substring(3), k.getValue()));
    }
    sb.append(rule).append('\n');
    return sb.toString();
  }

  private static String storeRow(String[] s) {
    // TIENDA id name city region format net units tickets avg tops
    String tops = s[10].replaceAll(" \\([0-9.]+\\)", "");
    return String.format(ES, "  %-5s %-34s %-18s %14s %,9d  %s%n", s[1], cut(s[2], 34), cut(s[4], 18),
        eur(Double.parseDouble(s[6])), Long.parseLong(s[8]), cut(tops, 70));
  }

  static final Locale ES = new Locale("es", "ES");

  static String fmt(double d) {
    return String.format(Locale.ROOT, "%.2f", d);
  }

  static String eur(double d) {
    return String.format(ES, "%,.2f €", d);
  }

  private static String cut(String s, int n) {
    return s.length() <= n ? s : s.substring(0, n - 1) + "…";
  }

  private static String bar(double ratio) {
    return repeat('#', (int) Math.round(ratio * 30));
  }

  private static String repeat(char c, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
