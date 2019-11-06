package ru.brykov.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.brykov.kafka.model.Message;
import ru.brykov.kafka.model.Messages;
import ru.brykov.kafka.service.ConsumerService;
import ru.brykov.kafka.service.CustomizedCrudRepository;
import ru.brykov.kafka.service.ProducerService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaTestApplication {

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaTestApplication.class, args);

        ProducerService producerService = context.getBean(ProducerService.class);
        ConsumerService consumerService = context.getBean(ConsumerService.class);
        CustomizedCrudRepository repository = context.getBean(CustomizedCrudRepository.class);


        runTestSingleMessage(producerService, consumerService);
        runTestMoreThenBatch(producerService, consumerService);
        runTestEmptyMessage(producerService, consumerService);
        runTestWrongMessageId(producerService, consumerService);
        runTestConstraintViolation(producerService, consumerService);

        Thread.sleep(7000);
        log.info(repository.findAll().toString());

        context.close();
    }

    private static void runTestSingleMessage(ProducerService producerService, ConsumerService consumerService) throws InterruptedException {
        Messages data = new Messages();
        ru.brykov.kafka.model.Message message = new ru.brykov.kafka.model.Message(1L, "Payload");
        data.setMessages(Collections.singletonList(message));

        producerService.sendMessage(data);
        consumerService.getLatch().await(10, TimeUnit.SECONDS);
    }

	private static void runTestMoreThenBatch(ProducerService producerService, ConsumerService consumerService) throws InterruptedException {
		Messages data = new Messages();
		List<Message> list = new ArrayList<>();
		for(int i = 10; i < 30; i++) {
			list.add(new ru.brykov.kafka.model.Message((long)i, "Payload"));
		}
		data.setMessages(list);

		producerService.sendMessage(data);
		consumerService.getLatch().await(10, TimeUnit.SECONDS);
	}

    private static void runTestEmptyMessage(ProducerService producerService, ConsumerService consumerService) throws InterruptedException {
        Messages data = new Messages();
        data.setMessages(Collections.emptyList());

        producerService.sendMessage(data);
        consumerService.getLatch().await(10, TimeUnit.SECONDS);
    }

    private static void runTestWrongMessageId(ProducerService producerService, ConsumerService consumerService) throws InterruptedException {
        Messages data = new Messages();
        ru.brykov.kafka.model.Message message = new ru.brykov.kafka.model.Message(-1L, "Payload");
        data.setMessages(Collections.singletonList(message));

        producerService.sendMessage(data);
        consumerService.getLatch().await(10, TimeUnit.SECONDS);
    }

    private static void runTestConstraintViolation(ProducerService producerService, ConsumerService consumerService) throws InterruptedException {
        Messages data = new Messages();
        data.setMessages(Arrays.asList(
        		new ru.brykov.kafka.model.Message(2L, "Payload"),
				new ru.brykov.kafka.model.Message(2L, "Payload")));

        producerService.sendMessage(data);
        consumerService.getLatch().await(10, TimeUnit.SECONDS);
    }
}
