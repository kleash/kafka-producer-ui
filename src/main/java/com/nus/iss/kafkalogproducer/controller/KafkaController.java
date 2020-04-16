package com.nus.iss.kafkalogproducer.controller;

import com.nus.iss.kafkalogproducer.executors.KafkaExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class KafkaController {

    private static final Logger LOGGER = LogManager.getLogger(KafkaController.class);

    @Autowired
    private KafkaExecutor kafkaExecutor;

    @GetMapping("/kafkawebmanager")
    public String kafkawebmanager() {
        return "kafkamanager";
    }

    @GetMapping("/kafkaproducer")
    public String kafkaproducer() {
        return "kafkaproducer";
    }

    @GetMapping("/kafkamanagertopics")
    public String kafkamanagertopics() {
        return "kafkamanagertopics";
    }

    @GetMapping("/kafkamanagercreatetopic")
    public String kafkamanagercreatetopic() {
        return "kafkamanagercreatetopic";
    }


    @PostMapping("/startapachelog")
    public String startapachelog(@RequestParam("time") int time,
                                 @RequestParam("records") int records,
                                 @RequestParam("recordspermin") int recordspermin,
                                 @RequestParam(value = "keeprunning", required = false) boolean keeprunning,
                                 Model model) {

        LOGGER.info("time {} records {} recordspermin {} keeprunning {} ", time, records, recordspermin, keeprunning);


        //records
        if (records > 0) {
            kafkaExecutor.submit(records);

            model.addAttribute("message", "Started the jobs. Will push " + records);
        } else
            //time and recordspermin
            if (time > 0 && recordspermin > 0) {

                kafkaExecutor.submit(time, recordspermin);
                model.addAttribute("message", "Started the jobs. Will push " + recordspermin + " per min for " + time + " seconds.");

            } else if (keeprunning && recordspermin > 0) {
                //recordspermin and keeprunning
                kafkaExecutor.submit(keeprunning, recordspermin);
                model.addAttribute("message", "Started the jobs. Will keep pushing the records on speed of " + recordspermin + "per min");
            } else {
                model.addAttribute("alertmessage", "Wrong Option");
            }


        return "kafkaproducer";
    }

    @GetMapping("/apachestop")
    public String apachestop(Model model) {
        kafkaExecutor.stopAllTasks();

        model.addAttribute("alertmessage", "Stopped the jobs");
        return "kafkaproducer";
    }
}
