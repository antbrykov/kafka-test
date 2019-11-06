package ru.brykov.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import ru.brykov.kafka.model.Messages;

@SpringBootApplication
public class KafkaTestApplication /*implements ApplicationRunner*/ {

//    @Autowired
//    private KafkaTemplate<String, Messages> kafkaTemplate;

	public static void main(String[] args) {
		SpringApplication.run(KafkaTestApplication.class, args);
	}

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        kafkaTemplate.send("group1", new Messages());
//    }
}
