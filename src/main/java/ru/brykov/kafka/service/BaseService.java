package ru.brykov.kafka.service;

import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import ru.brykov.kafka.model.Messages;
import ru.brykov.kafka.service.entity.Message;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public abstract class BaseService {

    protected List<Message> buffer = new CopyOnWriteArrayList<>();
    @Value(value = "${database.batch.size}")
    protected int bufferSize;
    @Value(value = "${database.batch.timeout}")
    protected long bufferTimeout;
    protected Instant startTime;
    @Autowired
    protected MessageDataService dataService;
    private CountDownLatch latch;

    protected void processMessage(Messages messages) {
        if (messages != null && messages.getMessages() != null && !messages.getMessages().isEmpty()) {
            startTime = Instant.now();
            for (ru.brykov.kafka.model.Message message : messages.getMessages()) {
                Long messageId = message.getMessageId();
                if (messageId != null && messageId >= 0) {
                    buffer.add(new Message(messageId, message.getPayload()));
                    if (buffer.size() == bufferSize) {
                        dataService.saveAll(buffer);
                        buffer.clear();
                    }
                } else {
                    getLogger().warn("messageId is wrong: {}", message);
                }
            }
            if (buffer.isEmpty()) {
                latch.countDown();
            }
        } else {
            getLogger().warn("empty message in kafka");
            latch.countDown();
        }
    }

    protected abstract Logger getLogger();

    @Scheduled(fixedRateString = "${database.batch.timeout}", initialDelay = 5000)
    private void writeOnTimeout() {
        getLogger().debug("start scheduled dump buffer with {} elements", buffer.size());
        if (startTime != null && Duration.between(startTime, Instant.now()).toMillis() > bufferTimeout && !buffer.isEmpty()) {
            dataService.saveAll(buffer);
            buffer.clear();
            startTime = null;
            getLogger().debug("dump empty");
            latch.countDown();
        }
        getLogger().debug("end dump buffer");
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }
}
