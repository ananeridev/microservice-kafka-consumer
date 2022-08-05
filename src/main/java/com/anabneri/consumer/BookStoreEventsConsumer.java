package com.anabneri.consumer;

import com.anabneri.service.BookStoreEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BookStoreEventsConsumer {

    // isso só vai impelementar depois de criar o service
    @Autowired
    private BookStoreEventsService bookStoreEventsService;


    // criar primeiro o KafkaListener e depois chamar o Service
    // Comentar sobre chave/valor
    @KafkaListener(topics = {"bookstore-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord)  throws JsonProcessingException {

        log.info("ConsumerRecord : {} ", consumerRecord);

        // enquanto nao tiver o service essa linha nao coloca
        bookStoreEventsService.processBookStoreEvent(consumerRecord);

    }
}
