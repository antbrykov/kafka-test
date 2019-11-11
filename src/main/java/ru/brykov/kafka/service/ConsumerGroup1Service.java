package ru.brykov.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.brykov.kafka.model.Messages;

@Service
@Slf4j
@ConditionalOnBean(name = {"group1ContainerFactory"})
public class ConsumerGroup1Service extends BaseService {

    @Value(value = "${kafka.group1.name}")
    private String groupName;

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group1.name}", containerFactory = "group1ContainerFactory")
    public void listener(Messages messages) {
        log.debug("start listener 1 " + groupName);
        processMessage(messages);
        log.debug("end listener 1 " + groupName);
    }

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group1.name}", containerFactory = "group1ContainerFactory")
    public void listener2(Messages messages) {
        log.debug("start listener 2 " + groupName);
        processMessage(messages);
        log.debug("end listener 2 " +  groupName);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
