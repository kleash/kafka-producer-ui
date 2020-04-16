package com.nus.iss.kafkalogproducer.executors;

import com.nus.iss.kafkalogproducer.kafka.KafkaProducer;

public class KafkaExecutorTask implements Runnable {

    private int records;
    private int time;
    private int recordspermin;
    private boolean keeprunning;
    private KafkaProducer kafkaProducer;

    public KafkaExecutorTask(KafkaProducer kafkaProducer, int records) {
        this.records = records;
        this.kafkaProducer = kafkaProducer;
    }

    public KafkaExecutorTask(KafkaProducer kafkaProducer, int time, int recodspermin) {
        this.time = time;
        this.recordspermin = recodspermin;
        this.kafkaProducer = kafkaProducer;
    }

    public KafkaExecutorTask(KafkaProducer kafkaProducer, boolean keeprunning, int recodspermin) {
        this.recordspermin = recodspermin;
        this.kafkaProducer = kafkaProducer;
        this.keeprunning = keeprunning;
    }

    @Override
    public void run() {
        if (records > 0) {
            //Send records
            kafkaProducer.sendApacheLog(records);

        } else if (keeprunning) {
            //Send recordspermin
            kafkaProducer.sendApacheLog(recordspermin);

        } else {
            //Send for specific time with records per min
            if (time > 60) {
                //Send records per min for time/60 times
                int count = 0;
                while (count < time / 60) {
                    //send message
                    kafkaProducer.sendApacheLog(recordspermin);
                    count++;
                }

            } else {
                //Just send records per min
                kafkaProducer.sendApacheLog(recordspermin);
            }

        }

    }
}