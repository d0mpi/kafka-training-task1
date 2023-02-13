package com.epam.training.task1.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class AtLeastOnceProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.topic}")
    private String topic;

    public void send(String key, String payload) {
        log.info("sending payload='{}' with key='{}'", payload, key);
        kafkaTemplate.send(topic, key, payload);
        kafkaTemplate.flush();
    }
}
