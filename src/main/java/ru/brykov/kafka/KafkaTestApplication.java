package ru.brykov.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.brykov.kafka.model.Message;
import ru.brykov.kafka.model.Messages;
import ru.brykov.kafka.service.BaseService;
import ru.brykov.kafka.service.ConsumerGroup1Service;
import ru.brykov.kafka.service.ConsumerGroup2Service;
import ru.brykov.kafka.service.DatabaseRepository;
import ru.brykov.kafka.service.ProducerService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaTestApplication {

    private static ProducerService producerService;
    private static ConsumerGroup1Service consumerGroup1Service;
    private static ConsumerGroup2Service consumerGroup2Service;
    private static DatabaseRepository repository;

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaTestApplication.class, args);

        producerService = context.getBean(ProducerService.class);
        consumerGroup1Service = context.containsBean("consumerGroup1Service") ? context.getBean(ConsumerGroup1Service.class) : null;
        consumerGroup2Service = context.containsBean("consumerGroup2Service") ? context.getBean(ConsumerGroup2Service.class) : null;
        repository = context.getBean(DatabaseRepository.class);


        runTestSingleMessage();
        runTestEmptyMessage();
        runTestWrongMessageId();
        runTestMoreThenBatch();
        runTestConstraintViolation();

        context.close();
    }

    private static void runTestSingleMessage() throws InterruptedException {
        log.debug("start testSingleMessage");
        CountDownLatch latch = resetLatch(consumerGroup1Service, consumerGroup2Service);

        Messages data = new Messages();
        ru.brykov.kafka.model.Message message = new ru.brykov.kafka.model.Message(1L, "Payload");
        data.setMessages(Collections.singletonList(message));

        producerService.sendMessage(data);
        latch.await(10, TimeUnit.SECONDS);
        log.debug(repository.findAll().toString());
        repository.deleteAll();
        log.debug("end testSingleMessage");
    }

    private static CountDownLatch resetLatch(BaseService... services) {
        int result = 0;
        for (BaseService service : services) {
            if (service != null)
                result += 1;
        }
        CountDownLatch latch = new CountDownLatch(result);
        for (BaseService service : services) {
            if (service != null)
                service.setLatch(latch);
        }
        return latch;
    }

    private static void runTestEmptyMessage() throws InterruptedException {
        log.debug("start testEmptyMessage");
        CountDownLatch latch = resetLatch(consumerGroup1Service, consumerGroup2Service);

        Messages data = new Messages();
        data.setMessages(Collections.emptyList());

        producerService.sendMessage(data);
        latch.await(10, TimeUnit.SECONDS);
        log.debug(repository.findAll().toString());
        repository.deleteAll();
        log.debug("end testEmptyMessage");
    }

    private static void runTestWrongMessageId() throws InterruptedException {
        log.debug("start testWrongMessageId");
        CountDownLatch latch = resetLatch(consumerGroup1Service, consumerGroup2Service);

        Messages data = new Messages();
        ru.brykov.kafka.model.Message message = new ru.brykov.kafka.model.Message(-1L, "Payload");
        data.setMessages(Collections.singletonList(message));

        producerService.sendMessage(data);
        latch.await(10, TimeUnit.SECONDS);
        log.debug(repository.findAll().toString());
        repository.deleteAll();
        log.debug("end testWrongMessageId");
    }

    private static void runTestMoreThenBatch() throws InterruptedException {
        log.debug("start testMoreThenBatch");
        CountDownLatch latch = resetLatch(consumerGroup1Service, consumerGroup2Service);

        List<Messages> manyData = new ArrayList<>();
        int step = 11;
        for (int y = 0; y < 35; y = y + step) {
            Messages data = new Messages();
            List<Message> list = new ArrayList<>();
            for (int i = y; i < y + step; i++) {
                list.add(new ru.brykov.kafka.model.Message((long) i, "Payload" + i));
            }
            data.setMessages(list);
            manyData.add(data);
        }

        for (Messages data : manyData) {
            producerService.sendMessage(data);
        }
        latch.await(10, TimeUnit.SECONDS);
        log.debug(repository.findAll().toString());
        repository.deleteAll();
        log.debug("end testMoreThenBatch");
    }

    private static void runTestConstraintViolation() throws InterruptedException {
        log.debug("start testConstraintViolation");
        CountDownLatch latch = resetLatch(consumerGroup1Service, consumerGroup2Service);

        List<Messages> manyData = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Messages data = new Messages();
            data.setMessages(Collections.singletonList(new Message(2L, "Payload")));
            manyData.add(data);
        }

        for (Messages data : manyData) {
            producerService.sendMessage(data);
        }
        latch.await(10, TimeUnit.SECONDS);
        log.debug(repository.findAll().toString());
        repository.deleteAll();
        log.debug("end testConstraintViolation");
    }
}
