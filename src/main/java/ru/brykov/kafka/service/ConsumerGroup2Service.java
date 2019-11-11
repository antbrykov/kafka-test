package ru.brykov.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.brykov.kafka.model.Messages;

@Slf4j
@Service
@ConditionalOnBean(name = {"group2ContainerFactory"})
public class ConsumerGroup2Service extends BaseService {

    @Value(value = "${kafka.group2.name}")
    private String groupName;

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${kafka.group2.name}", containerFactory = "group2ContainerFactory")
    public void listener(Messages messages) {
        log.debug("start listener 1 " + groupName);
        processMessage(messages);
        log.debug("end listener 1 " + groupName);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
