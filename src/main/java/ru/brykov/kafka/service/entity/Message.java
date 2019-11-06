package ru.brykov.kafka.service.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "MESSAGE")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Message {
    @Id
    private Long messageId;
    private String payload;
}
