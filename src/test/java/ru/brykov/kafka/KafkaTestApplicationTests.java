package ru.brykov.kafka;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import ru.brykov.kafka.appconfig.KafkaProducerConfig;
import ru.brykov.kafka.model.Message;
import ru.brykov.kafka.model.Messages;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

@SpringBootTest
class KafkaTestApplicationTests {

	@Autowired
	private KafkaTemplate<String, Messages> kafkaTemplate;
//	@Autowired
//	private CustomizedCrudRepository repository;

	@Test
	public void dummy() throws ExecutionException, InterruptedException {
		Messages data = new Messages();
		Message message = new Message();
		message.setMessageId(1);
		message.setPayload("Payload");
		data.setMessages(Collections.singletonList(message));
		SendResult<String, Messages> topicName = kafkaTemplate.send("topicName", data).get();
		System.out.println(topicName);
	}
}
