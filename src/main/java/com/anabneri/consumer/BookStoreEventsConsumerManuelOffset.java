package com.anabneri.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class BookStoreEventsConsumerManuelOffset implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = {"bookstore-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {

        log.info("ConsumerRecord : {} ", consumerRecord);
        acknowledgment.acknowledge();

    }
}
