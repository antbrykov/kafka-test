package ru.brykov.kafka.service;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import ru.brykov.kafka.service.entity.Message;

@Repository
public interface CustomizedCrudRepository extends CrudRepository<Message, Long> {

}
