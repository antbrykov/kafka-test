package ru.brykov.kafka.service.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "MESSAGE")
public class Message {
    @Id
    private Long messageId;
    private String payload;
}
