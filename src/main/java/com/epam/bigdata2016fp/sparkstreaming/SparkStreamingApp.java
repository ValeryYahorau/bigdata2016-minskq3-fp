package com.epam.bigdata2016fp.sparkstreaming;

import com.epam.bigdata2016fp.sparkstreaming.conf.AppProperties;
import com.epam.bigdata2016fp.sparkstreaming.hbase.HbaseProcessor;
import com.epam.bigdata2016fp.sparkstreaming.kafka.KafkaProcessor;
import com.epam.bigdata2016fp.sparkstreaming.model.CityInfo;
import com.epam.bigdata2016fp.sparkstreaming.model.ESModel;
import com.epam.bigdata2016fp.sparkstreaming.model.LogLine;
import com.epam.bigdata2016fp.sparkstreaming.utils.DictionaryUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.Map;


@ComponentScan
@EnableAutoConfiguration
public class SparkStreamingApp {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SparkStreamingApp.class).run(args);
        AppProperties props = ctx.getBean(AppProperties.class);
        JavaStreamingContext jsc = ctx.getBean(JavaStreamingContext.class);
        Map<String, CityInfo> dict = DictionaryUtils.citiesDictionry(props.getHadoop());
        System.out.println(dict.);

        Broadcast<Map<String, CityInfo>> brCitiesDict = jsc.sparkContext().broadcast(dict);

        JavaPairReceiverInputDStream<String, String> logs =
                KafkaProcessor.getStream(jsc, props.getKafkaConnection());

        //save to ELASTIC SEARCH
        String index = props.getElasticSearch().getIndex();
        String type = props.getElasticSearch().getType();
        String confStr = index + "/" + type;
        logs.map(keyValue -> {
            ESModel model = ESModel.parseLine(keyValue._2());
            CityInfo cityInfo = brCitiesDict.value().get(Integer.toString(model.getCity()));
            model.setGeoPoint(cityInfo);
            return model;
        }).map(ESModel::toStringifyJson).foreachRDD(jsonRdd -> JavaEsSpark.saveJsonToEs(jsonRdd, confStr));


        //save to HBASE
        JavaDStream<LogLine> logLineStream = logs.map(keyValue -> LogLine.parseLogLine(keyValue._2()));
        logLineStream
            .foreachRDD(rdd ->
                rdd.map(line -> LogLine.convertToPut(line, props.getHbase().getColumnFamily()))
                    .foreachPartition(iter -> HbaseProcessor.saveToTable(iter, props.getHbase()))
            );


        jsc.start();
        jsc.awaitTermination();
    }

}