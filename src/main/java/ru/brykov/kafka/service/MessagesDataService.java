package ru.brykov.kafka.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.brykov.kafka.service.entity.Message;

@Service
@Transactional
public class MessagesDataService {
    private CustomizedCrudRepository repository;

    public void save(Message messages) {
        repository.save(messages);
    }
}
