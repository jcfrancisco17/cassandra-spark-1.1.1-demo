package com.orangeandbronze.demo.cassandraspark.streaming;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by JC on 4/8/15.
 *
 */

public class CassandraSparkStreamingDemo {

    private final SparkConf conf;

    private CassandraSparkStreamingDemo(SparkConf conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("Java Streaming Demo");

        //The master URL to connect to, such as "local" to run locally with one thread,
        // "local[4]" to run locally with 4 cores,
        // or "spark://master:7077" to run on a Spark standalone cluster.
        conf.setMaster(args[0]);

        //Set a configuration variable.
        conf.set("spark.cassandra.connection.host", args[1]);

        CassandraSparkStreamingDemo demo = new CassandraSparkStreamingDemo(conf);
        demo.run();
    }

    private void run() {
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        generateData(sparkContext);
        sparkContext.stop();
    }

    private void generateData(JavaSparkContext sparkContext) {
        CassandraConnector connector = CassandraConnector.apply(sparkContext.getConf());

        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS wordcount");
            session.execute("CREATE KEYSPACE wordcount WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE wordcount.raw_word_data(line text, time timeuuid, primary key(line, time))");
        }

        ArrayList<RawLineData> lines = new ArrayList<>();
        lines.add(new RawLineData(UUIDs.timeBased(), "hello"));
        lines.add(new RawLineData(UUIDs.timeBased(), "hello world"));
        lines.add(new RawLineData(UUIDs.timeBased(), "hello kitty"));

        JavaRDD<RawLineData> rawLinesRDD = sparkContext.parallelize(lines);

        javaFunctions(rawLinesRDD).writerBuilder("wordcount", "raw_word_data", mapToRow(RawLineData.class)).saveToCassandra();
    }

    public static class RawLineData implements Serializable {
        private UUID time;
        private String line;

        public RawLineData(UUID time, String line) {
            this.time = time;
            this.line = line;
        }

        public UUID getTime() {
            return time;
        }

        public void setTime(UUID time) {
            this.time = time;
        }

        public String getLine() {
            return line;
        }

        public void setLine(String line) {
            this.line = line;
        }

        @Override
        public String toString() {
            return "RawLineData{" +
                    "time=" + time +
                    ", line='" + line + '\'' +
                    '}';
        }
    }


}
