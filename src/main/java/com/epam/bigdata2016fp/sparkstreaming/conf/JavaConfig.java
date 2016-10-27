package com.epam.bigdata2016fp.sparkstreaming.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(AppProperties.class)
public class JavaConfig {

    @Bean
    public SparkConf sparkConf(AppProperties conf) {
        return new SparkConf()
                .setAppName(conf.getSpark().getAppName());
    }

    @Bean
    public JavaStreamingContext streamingContext(SparkConf sparkConf, AppProperties conf) {
        int duration = conf.getSpark().getDuration();
        return new JavaStreamingContext(sparkConf, new Duration(duration));
    }


}
