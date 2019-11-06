package ru.brykov.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import ru.brykov.kafka.model.Message;
import ru.brykov.kafka.model.Messages;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@SpringBootTest
class KafkaTestApplicationTests {

    @Autowired
    private KafkaTemplate<String, Messages> kafkaTemplate;

	/**
	 * @deprecated look tests in {@link KafkaTestApplication#main(String[])}
	 */
    @Test
    public void dummy() throws ExecutionException, InterruptedException {
        Messages data = new Messages();
        Message message = new Message();
        message.setMessageId(1L);
        message.setPayload("Payload");
        data.setMessages(Collections.singletonList(message));
        SendResult<String, Messages> topicName = kafkaTemplate.send("topicName1", data).get();
        System.out.println("testMessage: " + topicName);
        Thread.sleep(15000);
    }
}
