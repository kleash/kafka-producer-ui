package com.nus.iss.kafkalogproducer.executors;

import com.nus.iss.kafkalogproducer.kafka.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class KafkaExecutor {

    private static final Logger LOGGER = LogManager.getLogger(KafkaExecutor.class);
    private ScheduledExecutorService executor;

    private List<Future<?>> tasks = new ArrayList<>();

    @Autowired
    private KafkaProducer kafkaProducer;

    @PostConstruct
    public void init() {
        executor = Executors.newScheduledThreadPool(4);
    }


    public void submit(int records) {
        LOGGER.debug("Records {}", records);
        KafkaExecutorTask task = new KafkaExecutorTask(kafkaProducer,records);
        Future<?> submit = executor.schedule(task, 0, TimeUnit.SECONDS);
        tasks.add(submit);
    }

    public void submit(int time, int recodspermin) {
        LOGGER.debug("time {} recodspermin{} ", time, recodspermin);
        KafkaExecutorTask task = new KafkaExecutorTask(kafkaProducer,time, recodspermin);
        Future<?> submit = executor.schedule(task, 0, TimeUnit.SECONDS);
        tasks.add(submit);
    }

    public void submit(boolean keeprunning, int recodspermin) {
        LOGGER.debug("keeprunning {} recodspermin{} ", keeprunning, recodspermin);
        KafkaExecutorTask task = new KafkaExecutorTask(kafkaProducer,keeprunning, recodspermin);
        Future<?> submit = executor.schedule(task, 60, TimeUnit.SECONDS);
        tasks.add(submit);
    }


    public void stopAllTasks() {
        tasks.stream().forEach(e -> e.cancel(true));
    }

    @Scheduled(fixedDelay = 5000)
    public void taskFinishChecker() {
        List<Future<?>> collect = tasks.stream().filter(e -> e.isDone()).collect(Collectors.toList());
        if (collect == null && !collect.isEmpty()) {
            tasks.removeAll(collect);
        }
    }

}
