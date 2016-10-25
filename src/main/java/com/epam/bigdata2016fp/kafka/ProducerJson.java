package com.epam.bigdata2016fp.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class ProducerJson {

    public static void main(String[] args) throws IOException {
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try (Stream<Path> paths = Files.walk(Paths.get(args[2]))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try (Stream<String> lines = Files.lines(filePath, Charset.forName("ISO-8859-1"))) {
                        lines.forEach(line -> {
                            line = "{\"adSlotWirdth\":300,\"streamId\":0,\"adExchange\":1,\"city\":87,\"ip\":\"180.127.189.*\",\"adSlotVisibility\":1,\"userAgent\":\"mozilla/4.0 (compatible; msie 6.0; windows nt 5.1; sv1; qqdownload 718)\",\"payingPrice\":0,\"biddingPrice\":227,\"adSlotHeight\":250,\"creativeId\":\"00fccc64a1ee2809348509b7ac2a97a5\",\"bidId\":\"b382c1c156dcbbd5b9317cb50f6a747b\",\"url\":\"249b2c34247d400ef1cd3c6bfda4f12a\",\"advertiserId\":3427,\"adSlotId\":\"mm_11402872_1272384_3182279\",\"adSlotFormat\":1,\"domain\":\"tFKETuqyMo1mjMp45SqfNX\",\"userTags\":282825712746,\"anonymousUrl\":\"\",\"ipinyouId\":\"Vh16OwT6OQNUXbj\",\"region\":80,\"timestLALA\":1462465635214}";
                            producer.send(new ProducerRecord<>(args[1], line));
                            System.out.println("Line was read from file: " + line);
                        });
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } finally {
            producer.close();
        }
    }
}