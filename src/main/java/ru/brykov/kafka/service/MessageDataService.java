package ru.brykov.kafka.service;

import ru.brykov.kafka.service.entity.Message;

public interface MessageDataService {
    void save(Message message);

    void saveAll(Iterable<Message> messages);
}
