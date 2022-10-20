package com.github.sroigmas.kafka.integration.testing.kafka;

import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
public class KafkaConsumer {

  private CountDownLatch latch = new CountDownLatch(1);
  private String data;

  @KafkaListener(topics = "${test.topic}")
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    log.info("Received data '{}'", consumerRecord.toString());
    data = consumerRecord.toString();
    latch.countDown();
  }

  public void resetLatch() {
    latch = new CountDownLatch(1);
  }
}
