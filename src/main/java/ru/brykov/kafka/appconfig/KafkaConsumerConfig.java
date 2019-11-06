package ru.brykov.kafka.appconfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.brykov.kafka.model.Messages;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Messages> groupFContainerFactory() {
        return containerFactory("group1");
    }

    @Bean
    @ConditionalOnProperty(name = "kafka.topic.partitions", havingValue = "2")
    public ConcurrentKafkaListenerContainerFactory<String, Messages> groupSContainerFactory() {
        return containerFactory("group2");
    }

    private ConcurrentKafkaListenerContainerFactory<String, Messages> containerFactory(String group) {
        ConcurrentKafkaListenerContainerFactory<String, Messages> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(group));
        return factory;
    }

    private ConsumerFactory<String, Messages> consumerFactory(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        JsonDeserializer<Messages> deserializer = new JsonDeserializer<>();
        deserializer.addTrustedPackages("*");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }
}
