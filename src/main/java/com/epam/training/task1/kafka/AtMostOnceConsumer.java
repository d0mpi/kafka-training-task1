package com.epam.training.task1.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@Slf4j
public class AtMostOnceConsumer {
    private final ConcurrentMap<String, String> receivedMessages = new ConcurrentHashMap<>();

    @KafkaListener(id = "testConsumer", topics = "${kafka.topic}")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("received payload='{}' with key='{}'", record.value(), record.key());
        ack.acknowledge();
        receivedMessages.put(record.key(), record.value());
    }

    public Map<String, String> getReceivedMessages() {
        return receivedMessages;
    }
}
