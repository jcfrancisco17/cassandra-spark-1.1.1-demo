import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions;
import com.datastax.spark.connector.japi.RDDJavaFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;


/**
 * Created by JC on 4/7/15.
 */
public class JavaDemo implements Serializable {
    private transient SparkConf conf;

    private JavaDemo(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
        compute(sc);
        showResults(sc);
        sc.stop();
    }

    private void generateData(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS java_api");
            session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE java_api.products(id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
            session.execute("CREATE TABLE java_api.sales(id UUID PRIMARY KEY, product INT, price DECIMAL)");
            session.execute("CREATE TABLE java_api.summaries(product INT PRIMARY KEY, summary DECIMAL)");
        }

        // Prepare the products hierarchy
        final List<Product> products = Arrays.asList(
                new Product(0, "All products", Collections.<Integer>emptyList()),
                new Product(1, "Product A", Arrays.asList(0)),
                new Product(4, "Product A1", Arrays.asList(0, 1)),
                new Product(5, "Product A2", Arrays.asList(0, 1)),
                new Product(2, "Product B", Arrays.asList(0)),
                new Product(6, "Product B1", Arrays.asList(0, 2)),
                new Product(7, "Product B2", Arrays.asList(0, 2)),
                new Product(3, "Product C", Arrays.asList(0)),
                new Product(8, "Product C1", Arrays.asList(0, 3)),
                new Product(9, "Product C2", Arrays.asList(0, 3))
        );

        JavaRDD<Product> productsRDD = sc.parallelize(products);
        javaFunctions(productsRDD).writerBuilder("java_api", "products", mapToRow(Product.class)).saveToCassandra();

        JavaRDD<Sale> salesRDD = productsRDD.filter(new Function<Product, Boolean>() {
            @Override
            public Boolean call(Product product) throws Exception {
                return product.getParents().size() == 2;
            }
        }).flatMap(new FlatMapFunction<Product, Sale>() {
            @Override
            public Iterable<Sale> call(Product product) throws Exception {
                Random random = new Random();
                List<Sale> sales = new ArrayList<>(1000);
                for (int i = 0; i < 1000; i++) {
                    sales.add(new Sale(UUID.randomUUID(), product.getId(), BigDecimal.valueOf(random.nextDouble())));
                }
                return sales;
            }
        });

        javaFunctions(salesRDD).writerBuilder("java_api", "sales", mapToRow(Sale.class)).saveToCassandra();;
    }

    private void compute(JavaSparkContext sc) {
    }

    private void showResults(JavaSparkContext sc) {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API Demo");
        conf.setMaster(args[0]);
        conf.set("spark.cassandra.connection.host", args[1]);

        JavaDemo app = new JavaDemo(conf);
        app.run();
    }

    public static class Product implements Serializable {
        private Integer id;
        private String name;
        private List<Integer> parents;

        public Product() { }

        public Product(Integer id, String name, List<Integer> parents) {
            this.id = id;
            this.name = name;
            this.parents = parents;
        }

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public List<Integer> getParents() { return parents; }
        public void setParents(List<Integer> parents) { this.parents = parents; }

        @Override
        public String toString() {
            return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name, parents);
        }
    }

    public static class Sale implements Serializable {
        private UUID id;
        private Integer product;
        private BigDecimal price;

        public Sale() { }

        public Sale(UUID id, Integer product, BigDecimal price) {
            this.id = id;
            this.product = product;
            this.price = price;
        }

        public UUID getId() { return id; }
        public void setId(UUID id) { this.id = id; }

        public Integer getProduct() { return product; }
        public void setProduct(Integer product) { this.product = product; }

        public BigDecimal getPrice() { return price; }
        public void setPrice(BigDecimal price) { this.price = price; }

        @Override
        public String toString() {
            return MessageFormat.format("Sale'{'id={0}, product={1}, price={2}'}'", id, product, price);
        }
    }

    public static class Summary implements Serializable {
        private Integer product;
        private BigDecimal summary;

        public Summary() { }

        public Summary(Integer product, BigDecimal summary) {
            this.product = product;
            this.summary = summary;
        }

        public Integer getProduct() { return product; }
        public void setProduct(Integer product) { this.product = product; }

        public BigDecimal getSummary() { return summary; }
        public void setSummary(BigDecimal summary) { this.summary = summary; }

        @Override
        public String toString() {
            return MessageFormat.format("Summary'{'product={0}, summary={1}'}'", product, summary);
        }
    }



}
