package com.demo.retail;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Generates one day of synthetic point-of-sale lines for the 400 stores, in parallel on YARN.
 *
 * <p>Each map task receives a range of stores (one line of a small control file) and writes the
 * ticket lines of those stores to /retail/raw/sales/dt=YYYY-MM-DD. The output deliberately
 * contains a few "dirty" records (corrupt lines, returns, late lines from the previous day) so the
 * ETL has something to clean.
 *
 * <p>Line format: ticket_id;timestamp;store_id;line_no;sku;qty;unit_price;discount_pct;payment
 */
public class GenerateSales extends Configured implements Tool {

  static final String CONF_DATE = "retail.date";
  static final String CONF_SCALE = "retail.scale";
  static final int PARTITIONS = 20;
  /** average tickets per day of a store with traffic 1.0 */
  static final int BASE_TICKETS = 2500;

  public static class GenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private List<RetailData.Store> stores;
    private List<RetailData.Product> products;
    /** cumulative product weights per region: each region has its own taste */
    private final Map<String, double[]> cumulativeByRegion = new java.util.HashMap<>();
    private LocalDate date;
    private double scale;
    private final Text out = new Text();
    private final StringBuilder sb = new StringBuilder(128);

    @Override
    protected void setup(Context ctx) {
      stores = RetailData.buildStores();
      products = RetailData.buildProducts();
      for (RetailData.Store s : stores) {
        cumulativeByRegion.computeIfAbsent(s.region, this::regionalWeights);
      }
      date = LocalDate.parse(ctx.getConfiguration().get(CONF_DATE));
      scale = ctx.getConfiguration().getDouble(CONF_SCALE, 1.0);
    }

    /** Stable per-region preferences: a category multiplier plus a strong local-favourite boost. */
    private double[] regionalWeights(String region) {
      Random r = new Random(region.hashCode());
      Map<String, Double> catFactor = new java.util.HashMap<>();
      double[] cumulative = new double[products.size()];
      double acc = 0;
      for (int i = 0; i < products.size(); i++) {
        RetailData.Product p = products.get(i);
        double cf = catFactor.computeIfAbsent(p.category, c -> 0.6 + r.nextDouble() * 1.2);
        double local = r.nextDouble() < 0.05 ? 3 + r.nextDouble() * 5 : 0.5 + r.nextDouble();
        acc += p.popularity * cf * local;
        cumulative[i] = acc;
      }
      return cumulative;
    }

    private RetailData.Product pickProduct(Random r, double[] cumulative) {
      double x = r.nextDouble() * cumulative[cumulative.length - 1];
      int i = java.util.Arrays.binarySearch(cumulative, x);
      return products.get(i >= 0 ? i : -i - 1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
      String[] f = value.toString().split(";");
      int from = Integer.parseInt(f[0]);
      int to = Integer.parseInt(f[1]);
      // weekends sell more
      int dow = date.getDayOfWeek().getValue();
      double dayFactor = dow == 6 ? 1.35 : dow == 5 ? 1.2 : dow == 7 ? 0.7 : 1.0;
      String day = date.toString();
      String prevDay = date.minusDays(1).toString();
      String compactDay = day.replace("-", "");

      for (int s = from; s < to; s++) {
        RetailData.Store store = stores.get(s);
        double[] cumulative = cumulativeByRegion.get(store.region);
        Random r = new Random(date.toEpochDay() * 1_000_003L + s);
        double storeDay = 0.85 + r.nextDouble() * 0.3; // this store's luck today
        int tickets = (int) (BASE_TICKETS * store.traffic * dayFactor * storeDay * scale);
        for (int t = 1; t <= tickets; t++) {
          // opening hours 08:00-22:00 with lunch and evening peaks
          int minute = openingMinute(r);
          String ts = String.format(Locale.ROOT, "%sT%02d:%02d:%02d", day, 8 + minute / 60, minute % 60, r.nextInt(60));
          if (r.nextDouble() < 0.002) {
            ts = prevDay + "T21:" + String.format(Locale.ROOT, "%02d:%02d", r.nextInt(60), r.nextInt(60)); // late upload
          }
          String ticketId = String.format(Locale.ROOT, "%s-%s-%06d", store.id, compactDay, t);
          String payment = r.nextDouble() < 0.72 ? "TARJETA" : r.nextDouble() < 0.6 ? "EFECTIVO" : "APP";
          int lines = 1 + (int) Math.min(24, -Math.log(1 - r.nextDouble()) * 5.5);
          for (int l = 1; l <= lines; l++) {
            if (r.nextDouble() < 0.0005) {
              out.set("#ERR#" + ticketId + ";;" + r.nextInt(1000) + ";NaN"); // corrupt record from a till
              ctx.write(NullWritable.get(), out);
              ctx.getCounter("Generador", "Lineas corruptas").increment(1);
              continue;
            }
            RetailData.Product p = pickProduct(r, cumulative);
            int qty = 1 + (r.nextDouble() < 0.25 ? r.nextInt(4) : 0);
            if (r.nextDouble() < 0.004) {
              qty = -qty; // return
            }
            double price = p.price * (0.97 + r.nextDouble() * 0.06);
            double d = r.nextDouble();
            int discount = d < 0.80 ? 0 : d < 0.92 ? 10 : d < 0.98 ? 20 : 30;
            sb.setLength(0);
            sb.append(ticketId).append(';').append(ts).append(';').append(store.id).append(';').append(l)
                .append(';').append(p.sku).append(';').append(qty).append(';')
                .append(String.format(Locale.ROOT, "%.2f", price)).append(';').append(discount).append(';')
                .append(payment);
            out.set(sb.toString());
            ctx.write(NullWritable.get(), out);
          }
          ctx.getCounter("Generador", "Tickets").increment(1);
          ctx.getCounter("Generador", "Lineas").increment(lines);
          if (t % 5000 == 0) {
            ctx.progress();
          }
        }
        ctx.setStatus("Tienda " + store.id + " generada");
      }
    }

    private static int openingMinute(Random r) {
      double x = r.nextDouble();
      double peak = x < 0.35 ? 5.0 * 60 : x < 0.75 ? 11.0 * 60 : 8.0 * 60; // 13:00, 19:00 or anywhere
      double m = x < 0.75 ? peak + r.nextGaussian() * 80 : r.nextDouble() * 14 * 60;
      return (int) Math.max(0, Math.min(14 * 60 - 1, m));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Map<String, String> opts = RetailMain.parseOpts(args);
    LocalDate date = RetailMain.dateOpt(opts);
    double scale = Double.parseDouble(opts.getOrDefault("scale", "1.0"));

    Configuration conf = getConf();
    conf.set(CONF_DATE, date.toString());
    conf.setDouble(CONF_SCALE, scale);
    FileSystem fs = FileSystem.get(conf);

    RetailData.writeMaster(fs);
    System.out.println("Maestros escritos: " + RetailData.STORES_FILE + ", " + RetailData.PRODUCTS_FILE);

    // control file: one line per map task with a range of stores
    Path control = new Path(RetailData.BASE_DIR + "/tmp/generate-" + date + ".txt");
    List<String> ranges = new ArrayList<>();
    int per = RetailData.NUM_STORES / PARTITIONS;
    for (int i = 0; i < PARTITIONS; i++) {
      ranges.add((i * per) + ";" + (i == PARTITIONS - 1 ? RetailData.NUM_STORES : (i + 1) * per));
    }
    Collections.shuffle(ranges, new Random(1)); // spread the big stores across tasks
    try (Writer w = new OutputStreamWriter(fs.create(control, true), StandardCharsets.UTF_8)) {
      for (String line : ranges) {
        w.write(line + "\n");
      }
    }

    Path out = new Path(RetailData.RAW_DIR + "/dt=" + date);
    if (fs.exists(out)) {
      fs.delete(out, true);
    }

    Job job = Job.getInstance(conf, "Retail - generar ventas sinteticas " + date);
    job.setJarByClass(GenerateSales.class);
    job.setMapperClass(GenMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, 1);
    FileInputFormat.addInputPath(job, control);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, out);

    long t0 = System.currentTimeMillis();
    boolean ok = job.waitForCompletion(true);
    fs.delete(control, false);
    if (ok) {
      long lines = job.getCounters().findCounter("Generador", "Lineas").getValue();
      long tickets = job.getCounters().findCounter("Generador", "Tickets").getValue();
      System.out.printf(Locale.ROOT, "%nGenerados %,d tickets y %,d lineas de venta para %s en %s (%.1f s)%n",
          tickets, lines, date, out, (System.currentTimeMillis() - t0) / 1000.0);
    }
    return ok ? 0 : 1;
  }
}
