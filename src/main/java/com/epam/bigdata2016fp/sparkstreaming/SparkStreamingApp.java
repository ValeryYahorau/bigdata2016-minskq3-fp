package com.epam.bigdata2016fp.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.HashMap;
import java.util.Map;

public class SparkStreamingApp {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingLogAggregationApp {zkQuorum} {group} {topic} {numThreads}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingLogAggregationApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {
            return new String(tuple2._2());
        });

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaEsSpark.saveToEs(stringJavaRDD, "test/test");
            }
        });

        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}