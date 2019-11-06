package ru.brykov.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.brykov.kafka.model.Messages;

@Service
public class ProducerService {

    @Value(value = "${kafka.topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Messages> kafkaTemplate;

    public void sendMessage(Messages messages) {
        kafkaTemplate.send(topicName, messages);
    }
}
