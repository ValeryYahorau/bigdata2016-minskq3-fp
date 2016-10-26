package com.epam.bigdata2016fp.sparkstreaming;

import com.epam.bigdata2016fp.sparkstreaming.entity.CityInfoEntity;
import com.epam.bigdata2016fp.sparkstreaming.entity.LogsEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingApp {zkQuorum} {group} {topic} {numThreads}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {
            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());
            CityInfoEntity cie = new CityInfoEntity();
            cie.setLat(Float.parseFloat("42.626595"));
            cie.setLon(Float.parseFloat("-0.488439"));
            logsEntity.setGeoPoint(cie);
            JSONObject jsonObject = new JSONObject(logsEntity);
            jsonObject.append("@sended_at",new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(new Date()));


            String json  =jsonObject.toString();


            String jsonStr ="{\"bidId\":null,\"timestamp\":null,\"ipinyouId\":null,\"userAgent\":null,\"ip\":null,\"region\":0,\"city\":0,\"adExchange\":0,\"domain\":null,\"url\":null,\"anonymousUrl\":null,\"adSlotId\":null,\"adSlotWirdth\":0,\"adSlotHeight\":0,\"adSlotVisibility\":0,\"adSlotFormat\":0,\"payingPrice\":0,\"creativeId\":null,\"biddingPrice\":0,\"advertiserId\":0,\"userTags\":0,\"streamId\":0,\"geoPoint1\":{\"geohash\":\"ezrm5c0vx832\"}}";
            JSONObject jsonObject2 = new JSONObject(jsonStr);
            jsonObject2.append("@sended_at",new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(new Date()));

            System.out.println("########### JSON");
            System.out.println(jsonObject2.toString());
            return jsonObject2.toString();
        });

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaEsSpark.saveJsonToEs(stringJavaRDD, "logs7/input7");
            }
        });

//        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
//        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";
//
//
//        JavaRDD<String> stringRDD = jssc.parallelize(ImmutableList.of(json1, json2));
//        JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");

        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}