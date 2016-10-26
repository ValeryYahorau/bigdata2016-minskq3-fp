package com.epam.bigdata2016fp.sparkstreaming;

import com.epam.bigdata2016fp.sparkstreaming.entity.CityInfoEntity;
import com.epam.bigdata2016fp.sparkstreaming.entity.LogsEntity;
import com.epam.bigdata2016fp.sparkstreaming.helper.FileHelper;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkStreamingApp {

    private static final String SPLIT = "\\t";
    private static final SimpleDateFormat LOGS_DATE_FORMAT = new SimpleDateFormat("yyyyMMddhhmmss");
    private static final SimpleDateFormat JSON_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");


    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.err.println("Usage: SparkStreamingApp {zkQuorum} {group} {topic} {numThreads} {cityPath} {index} {type}");
            System.exit(1);
        }

        String zkQuorum = args[0];
        String group = args[1];
        String[] topics = args[2].split(",");
        int numThreads = Integer.parseInt(args[3]);


        List<String> allCities = FileHelper.getLinesFromFile(args[4]);
        HashMap<Integer, CityInfoEntity> cityInfoMap = new HashMap<>();
        allCities.forEach(city -> {
            String[] fields = city.split(SPLIT);
            cityInfoMap.put(Integer.parseInt(fields[0]), /*geoPoint*/new CityInfoEntity(Float.parseFloat(fields[6]), Float.parseFloat(fields[7])));
        });


        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingApp");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("es.index.auto.create", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Broadcast<Map<Integer, CityInfoEntity>> broadcastVar = jssc.sparkContext().broadcast(cityInfoMap);

        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(tuple2 -> {

            LogsEntity logsEntity = new LogsEntity(tuple2._2().toString());

            Date date = LOGS_DATE_FORMAT.parse(logsEntity.getTimestamp());
            logsEntity.setTimestamp(JSON_DATE_FORMAT.format(date));

            logsEntity.setGeoPoint(broadcastVar.value().get(logsEntity.getCity()));
            UserAgent ua = UserAgent.parseUserAgentString(tuple2._2().toString());
            String device = ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
            String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
            String uaFamily = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
            logsEntity.setDevice(device);
            logsEntity.setOsName(osName);
            logsEntity.setUaFamily(uaFamily);

            JSONObject jsonObject = new JSONObject(logsEntity);
            jsonObject.append("@sended_at", new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(new Date()));

            String json = jsonObject.toString();
            System.out.println("####");

            System.out.println(json);
            return json;
        });

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                JavaEsSpark.saveJsonToEs(stringJavaRDD, args[5] + "/" + args[6]);
            }
        });

        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}