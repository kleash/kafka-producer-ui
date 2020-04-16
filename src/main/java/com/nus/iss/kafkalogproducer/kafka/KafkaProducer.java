package com.nus.iss.kafkalogproducer.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Component
public class KafkaProducer {

    private static final Logger LOGGER = LogManager.getLogger(KafkaProducer.class);
    @Value(value = "${topic}")
    private String topic;
    @Value(value = "${apachelogfile}")
    private String apachelogfile;


    private LineIterator it;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void test() throws IOException {
        String home = System.getProperty("user.home");
        String replace = apachelogfile.replace("~", home);
        LOGGER.info("APACHE LOG READ: "+replace);
        it = FileUtils.lineIterator(new File(replace), "UTF-8");
    }

    public synchronized void sendApacheLog(int records) {

        try {
            int count = 0;
            while (count < records) {
                if (it.hasNext()) {
                    sendMessage(it.nextLine());
                    count++;
                } else {
                    it.close();
                    LOGGER.info("CLOSING FILE. REACHED TO END OF APACHE FILE");
                    String home = System.getProperty("user.home");
                    String replace = apachelogfile.replace("~", home);
                    it = FileUtils.lineIterator(new File(replace), "UTF-8");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void sendMessage(String message) {

        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
    }
}
