package ru.brykov.kafka.service;

import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.brykov.kafka.model.Messages;
import ru.brykov.kafka.service.entity.Message;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

@Service
@Slf4j
public class ConsumerService {

    public synchronized CountDownLatch getLatch() {
        if (latch.getCount() == 0) {
            latch = new CountDownLatch(partitions);
        }
        return latch;
    }

    private static final List<Message> buffer = new CopyOnWriteArrayList<>();

    @Value(value = "${kafka.topic.partitions}")
    private int partitions;

    private CountDownLatch latch = new CountDownLatch(partitions);

    @Value(value = "${database.batch.size}")
    private int bufferSize;

    @Value(value = "${database.batch.timeout}")
    private long bufferTimeout;

    private Instant startTime;

    @Autowired
    private MessagesDataService dataService;

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "group1", containerFactory = "groupFContainerFactory")
    public void listener(Messages messages) {
        log.info("start listener group 1");
        processMessage(messages);
        log.info("end listener group 1");
    }

//    @KafkaListener(topics = "${kafka.topic.name}", groupId = "group2", containerFactory = "groupSContainerFactory")
//    public void listener2(Messages messages) {
//        log.info("start listener group 2");
//        processMessage(messages);
//        log.info("start listener group 2");
//    }

    private void processMessage(Messages messages) {
        if (messages != null && messages.getMessages() != null && !messages.getMessages().isEmpty()) {
            startTime = Instant.now();
            for (ru.brykov.kafka.model.Message message : messages.getMessages()) {
                Long messageId = message.getMessageId();
                if (messageId != null && messageId >= 0) {
                    buffer.add(new Message(messageId, message.getPayload()));
                } else {
                    log.warn("messageId is wrong: {}", message);
                }
            }
            if (buffer.size() >= bufferSize) {
                dataService.saveAll(buffer);
                buffer.clear();
            }
        } else {
            log.warn("empty message in kafka");
        }
        latch.countDown();
    }

    @Scheduled(fixedRateString = "${database.batch.timeout}")
    private void writeOnTimeout() {
        log.info("start scheduled dump buffer with {} elements", buffer.size());
        if (startTime != null && Duration.between(startTime, Instant.now()).toMillis() > bufferTimeout) {
            dataService.saveAll(buffer);
            buffer.clear();
            startTime = null;
            log.info("dump empty");
        }
        log.info("end dump buffer");
    }
}
