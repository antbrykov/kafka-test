package ru.brykov.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.brykov.kafka.service.entity.Message;

@Service
@Transactional
@Slf4j
public class MessageDataServiceImpl implements MessageDataService {
    @Autowired
    private DatabaseRepository repository;

    @Override
    public void save(Message message) {
        repository.save(message);
    }

    @Override
    public void saveAll(Iterable<Message> messages) {
        try {
            repository.saveAll(messages);
        } catch (Exception e) {
            log.error("error save into db", e);
        }
    }
}
