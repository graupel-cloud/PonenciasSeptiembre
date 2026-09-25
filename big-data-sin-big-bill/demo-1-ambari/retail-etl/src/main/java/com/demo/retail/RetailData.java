package com.demo.retail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Synthetic master data for the chain: 400 stores and ~550 products.
 * Generated deterministically from a fixed seed so every run describes the same chain.
 */
public final class RetailData {

  public static final int NUM_STORES = 400;
  public static final String BASE_DIR = "/retail";
  public static final String MASTER_DIR = BASE_DIR + "/master";
  public static final String STORES_FILE = MASTER_DIR + "/stores.csv";
  public static final String PRODUCTS_FILE = MASTER_DIR + "/products.csv";
  public static final String RAW_DIR = BASE_DIR + "/raw/sales";
  public static final String CURATED_DIR = BASE_DIR + "/curated/daily_sales";
  public static final String REPORT_DIR = BASE_DIR + "/reports";

  private static final long MASTER_SEED = 20260401L;

  private static final String[][] CITIES = {
      {"Madrid", "Madrid"}, {"Alcalá de Henares", "Madrid"}, {"Getafe", "Madrid"}, {"Móstoles", "Madrid"},
      {"Barcelona", "Cataluña"}, {"L'Hospitalet", "Cataluña"}, {"Badalona", "Cataluña"}, {"Tarragona", "Cataluña"},
      {"Girona", "Cataluña"}, {"Lleida", "Cataluña"}, {"Valencia", "C. Valenciana"}, {"Alicante", "C. Valenciana"},
      {"Elche", "C. Valenciana"}, {"Castellón", "C. Valenciana"}, {"Sevilla", "Andalucía"}, {"Málaga", "Andalucía"},
      {"Córdoba", "Andalucía"}, {"Granada", "Andalucía"}, {"Almería", "Andalucía"}, {"Cádiz", "Andalucía"},
      {"Bilbao", "País Vasco"}, {"Vitoria", "País Vasco"}, {"San Sebastián", "País Vasco"},
      {"Zaragoza", "Aragón"}, {"Huesca", "Aragón"}, {"Valladolid", "Castilla y León"}, {"Burgos", "Castilla y León"},
      {"León", "Castilla y León"}, {"Salamanca", "Castilla y León"}, {"Toledo", "Castilla-La Mancha"},
      {"Albacete", "Castilla-La Mancha"}, {"A Coruña", "Galicia"}, {"Vigo", "Galicia"}, {"Santiago", "Galicia"},
      {"Oviedo", "Asturias"}, {"Gijón", "Asturias"}, {"Santander", "Cantabria"}, {"Pamplona", "Navarra"},
      {"Logroño", "La Rioja"}, {"Murcia", "Murcia"}, {"Cartagena", "Murcia"}, {"Palma", "Baleares"},
      {"Las Palmas", "Canarias"}, {"Santa Cruz de Tenerife", "Canarias"}, {"Badajoz", "Extremadura"}
  };

  /** category, min price, max price, then item names */
  private static final String[][] CATEGORIES = {
      {"Frutas y verduras", "0.60", "4.50", "Manzana Golden", "Plátano de Canarias", "Tomate rama", "Lechuga iceberg",
          "Naranja zumo", "Patata lavada", "Cebolla", "Pimiento rojo", "Aguacate", "Fresón"},
      {"Carne y charcutería", "2.50", "18.00", "Pechuga de pollo", "Filete de ternera", "Lomo de cerdo",
          "Jamón serrano", "Chorizo ibérico", "Pavo en lonchas", "Hamburguesa mixta", "Salchichas frescas"},
      {"Pescadería", "3.00", "22.00", "Salmón fresco", "Merluza", "Gambas", "Atún en aceite", "Bacalao desalado",
          "Mejillones", "Sardinas"},
      {"Lácteos y huevos", "0.70", "6.50", "Leche entera", "Leche semidesnatada", "Yogur natural", "Queso curado",
          "Queso fresco", "Mantequilla", "Huevos camperos", "Nata para cocinar", "Kéfir"},
      {"Panadería", "0.40", "4.00", "Barra de pan", "Pan de molde", "Croissant", "Magdalenas", "Pan integral",
          "Baguette"},
      {"Despensa", "0.50", "7.00", "Aceite de oliva virgen extra", "Arroz redondo", "Macarrones", "Lentejas",
          "Garbanzos", "Tomate frito", "Café molido", "Galletas María", "Cereales", "Azúcar", "Harina"},
      {"Bebidas", "0.35", "9.00", "Agua mineral", "Refresco de cola", "Cerveza", "Zumo de naranja", "Vino tinto",
          "Vino blanco", "Bebida isotónica", "Refresco de limón"},
      {"Congelados", "1.20", "8.50", "Pizza congelada", "Guisantes", "Helado de vainilla", "Croquetas",
          "Varitas de merluza", "Verduras para salteado"},
      {"Droguería", "0.90", "12.00", "Detergente líquido", "Suavizante", "Lavavajillas", "Lejía",
          "Papel higiénico", "Rollo de cocina", "Limpiahogar"},
      {"Higiene personal", "1.00", "10.00", "Champú", "Gel de ducha", "Pasta de dientes", "Desodorante",
          "Crema hidratante", "Cepillo de dientes"},
      {"Mascotas", "1.50", "25.00", "Pienso perro", "Pienso gato", "Arena para gatos", "Snacks perro"}
  };

  private static final String[] VARIANTS = {"Marca propia", "Premium", "Eco", "Pack ahorro", "Formato familiar",
      "Clásico", "Origen España", "Selección"};

  private RetailData() {
  }

  /** A store of the chain. traffic is a relative multiplier of daily tickets. */
  public static final class Store {
    public final String id;
    public final String name;
    public final String city;
    public final String region;
    public final String format;
    public final double traffic;

    Store(String id, String name, String city, String region, String format, double traffic) {
      this.id = id;
      this.name = name;
      this.city = city;
      this.region = region;
      this.format = format;
      this.traffic = traffic;
    }

    String toCsv() {
      return String.join(";", id, name, city, region, format, String.format(Locale.ROOT, "%.3f", traffic));
    }

    static Store fromCsv(String line) {
      String[] f = line.split(";", -1);
      return new Store(f[0], f[1], f[2], f[3], f[4], Double.parseDouble(f[5]));
    }
  }

  /** A product (SKU). popularity drives how often it is picked in a ticket. */
  public static final class Product {
    public final String sku;
    public final String name;
    public final String category;
    public final double price;
    public final double popularity;

    Product(String sku, String name, String category, double price, double popularity) {
      this.sku = sku;
      this.name = name;
      this.category = category;
      this.price = price;
      this.popularity = popularity;
    }

    String toCsv() {
      return String.join(";", sku, name, category, String.format(Locale.ROOT, "%.2f", price),
          String.format(Locale.ROOT, "%.6f", popularity));
    }

    static Product fromCsv(String line) {
      String[] f = line.split(";", -1);
      return new Product(f[0], f[1], f[2], Double.parseDouble(f[3]), Double.parseDouble(f[4]));
    }
  }

  public static List<Store> buildStores() {
    Random r = new Random(MASTER_SEED);
    List<Store> stores = new ArrayList<>(NUM_STORES);
    Map<String, Integer> perCity = new HashMap<>();
    for (int i = 1; i <= NUM_STORES; i++) {
      // big cities get more stores
      String[] city = CITIES[Math.min(CITIES.length - 1, (int) Math.floor(Math.pow(r.nextDouble(), 1.6) * CITIES.length))];
      double p = r.nextDouble();
      String format;
      double traffic;
      if (p < 0.10) {
        format = "Hipermercado";
        traffic = 2.8 + r.nextDouble() * 0.8;
      } else if (p < 0.70) {
        format = "Supermercado";
        traffic = 0.8 + r.nextDouble() * 0.5;
      } else {
        format = "Express";
        traffic = 0.35 + r.nextDouble() * 0.25;
      }
      int n = perCity.merge(city[0], 1, Integer::sum);
      String id = String.format(Locale.ROOT, "T%03d", i);
      String name = String.format(Locale.ROOT, "%s %s %d", format, city[0], n);
      stores.add(new Store(id, name, city[0], city[1], format, traffic));
    }
    return stores;
  }

  public static List<Product> buildProducts() {
    Random r = new Random(MASTER_SEED + 1);
    List<Product> products = new ArrayList<>();
    int n = 0;
    for (String[] cat : CATEGORIES) {
      double min = Double.parseDouble(cat[1]);
      double max = Double.parseDouble(cat[2]);
      for (int i = 3; i < cat.length; i++) {
        for (String variant : VARIANTS) {
          if (r.nextDouble() < 0.25) {
            continue; // not every item exists in every variant
          }
          n++;
          double price = Math.round((min + Math.pow(r.nextDouble(), 1.5) * (max - min)) * 100) / 100.0;
          // Zipf-like popularity: a few products sell a lot, the long tail sells little
          double popularity = 1.0 / Math.pow(1 + r.nextInt(400), 0.6);
          if (cat[0].equals("Panadería") || cat[0].equals("Lácteos y huevos") || cat[0].equals("Frutas y verduras")) {
            popularity *= 2.5; // everyday items
          }
          products.add(new Product(String.format(Locale.ROOT, "P%05d", n), cat[i] + " " + variant, cat[0], price,
              popularity));
        }
      }
    }
    return products;
  }

  public static void writeMaster(FileSystem fs) throws IOException {
    try (Writer w = new OutputStreamWriter(fs.create(new Path(STORES_FILE), true), StandardCharsets.UTF_8)) {
      for (Store s : buildStores()) {
        w.write(s.toCsv());
        w.write('\n');
      }
    }
    try (FSDataOutputStream out = fs.create(new Path(PRODUCTS_FILE), true);
         Writer w = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
      for (Product p : buildProducts()) {
        w.write(p.toCsv());
        w.write('\n');
      }
    }
  }

  public static Map<String, Store> readStores(FileSystem fs, Path path) throws IOException {
    Map<String, Store> m = new HashMap<>();
    for (String line : readLines(fs, path)) {
      Store s = Store.fromCsv(line);
      m.put(s.id, s);
    }
    return m;
  }

  public static Map<String, Product> readProducts(FileSystem fs, Path path) throws IOException {
    Map<String, Product> m = new HashMap<>();
    for (String line : readLines(fs, path)) {
      Product p = Product.fromCsv(line);
      m.put(p.sku, p);
    }
    return m;
  }

  private static List<String> readLines(FileSystem fs, Path path) throws IOException {
    List<String> lines = new ArrayList<>();
    try (FSDataInputStream in = fs.open(path);
         BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (!line.isEmpty()) {
          lines.add(line);
        }
      }
    }
    return lines;
  }
}
