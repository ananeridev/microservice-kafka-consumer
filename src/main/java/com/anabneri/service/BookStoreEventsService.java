package com.anabneri.service;

import com.anabneri.entity.BookStoreEvent;
import com.anabneri.jpa.BookStoreEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class BookStoreEventsService {

    // uso pq quero retornar um objeto da minha string
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
   private BookStoreEventRepository bookStoreEventRepository;

    public void processBookStoreEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        BookStoreEvent bookStoreEvent =  objectMapper.readValue(consumerRecord.value(), BookStoreEvent.class);

       log.info("bookStoreEvent : {}", bookStoreEvent);

       if (bookStoreEvent.getBookStoreEventId()!=null && bookStoreEvent.getBookStoreEventId()==000) {

           throw new RecoverableDataAccessException("Temporary network Issue");
       }

       switch (bookStoreEvent.getBookStoreEventType()) {
           case NEW:
               // salva essa operacao
               save(bookStoreEvent);
               break;
           case UPDATE:
               // valida o book store event
               validate(bookStoreEvent);

               // salva no banco
               save(bookStoreEvent);
               break;
           default:
               log.info("BookStoreEventType invalid");
       }
    }

    public void handleRecovery(ConsumerRecord<Integer,String> record){

        Integer key = record.key();
        String message = record.value();

        ListenableFuture<SendResult<Integer,String>> listenableFuture = kafkaTemplate.sendDefault(key, message);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, message, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, message, result);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }


    private void validate(BookStoreEvent bookStoreEvent) {
        if (bookStoreEvent.getBookStoreEventId() == null) {
            throw new IllegalArgumentException("Book Store Event Id is missing, please put an Id");
        }
        Optional<BookStoreEvent> bookStoreEventOptional = bookStoreEventRepository.findById(bookStoreEvent.getBookStoreEventId());
        if(!bookStoreEventOptional.isPresent()) {
            throw  new IllegalArgumentException("Not a valid BookStoreEvent");
        }
        log.info("Validation is successful for the BookStore Event : {}", bookStoreEventOptional.get());
    }

    private void save(BookStoreEvent bookStoreEvent) {
        bookStoreEvent.getBook().setBookStoreEvent(bookStoreEvent);
        bookStoreEventRepository.save(bookStoreEvent);
        log.info("Persisted the BookStoreEvent Successfully {}", bookStoreEvent);
    }
}
