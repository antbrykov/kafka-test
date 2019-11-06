package ru.brykov.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.h2.jdbc.JdbcBatchUpdateException;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.brykov.kafka.service.entity.Message;

@Service
@Transactional
@Slf4j
public class MessagesDataService {
    @Autowired
    private CustomizedCrudRepository repository;

    public void save(Message message) {
        repository.save(message);
    }

    public void saveAll(Iterable<Message> messages) {
        try {
            repository.saveAll(messages);
        } catch (DataIntegrityViolationException | ConstraintViolationException e) {
            log.error("error save into db", e);
        }
    }
}
