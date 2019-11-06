package ru.brykov.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.brykov.kafka.model.Messages;

//@Component
public class Listener {

    @Autowired
    private MessagesDataService dataService;

//    @KafkaListener(topics = "topicName", groupId = "group1", containerFactory = "kafkaListenerContainerFactory")
//    public void listener1(@Payload Messages message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
//        System.out.println("Received Message in group group1: " + message + " from partition " + partition);
//    }

    @KafkaListener(topics = "topicName", containerFactory = "kafkaListenerContainerFactory")
    public void listener2(Messages messages) {
        System.out.println("Received Message in group group1: " + messages);
        throw new RuntimeException();
    }
}
