package com.epam.training.task1.kafka;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration(initializers = {KafkaTest.Initializer.class})
public class KafkaTest {

    @Autowired
    private AtMostOnceConsumer consumer;

    @Autowired
    private AtLeastOnceProducer producer;

    @ClassRule
    public static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertyValues.of(
                    "kafka.bootstrapServer=" + kafka.getBootstrapServers(), "kafka.topic=test_topic"
            ).applyTo(applicationContext.getEnvironment());
        }
    }

    @Test
    public void testReceiving()
            throws Exception {
        producer.send("key", "payload");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> !consumer.getReceivedMessages().isEmpty());
        Assertions.assertEquals("payload", consumer.getReceivedMessages().get("key"));
    }
}
