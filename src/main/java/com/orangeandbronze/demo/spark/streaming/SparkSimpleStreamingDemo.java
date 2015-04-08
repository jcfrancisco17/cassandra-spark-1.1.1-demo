package com.orangeandbronze.demo.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by JC on 4/8/15.
 *
 * Based on:
 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#a-quick-example
 */
public class SparkSimpleStreamingDemo implements Serializable {

    private transient SparkConf conf;

    private SparkSimpleStreamingDemo(SparkConf conf) {
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

        SparkSimpleStreamingDemo demo = new SparkSimpleStreamingDemo(conf);
        demo.run();
    }

    private void run() {

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) throws Exception {
                return Arrays.asList(x.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
