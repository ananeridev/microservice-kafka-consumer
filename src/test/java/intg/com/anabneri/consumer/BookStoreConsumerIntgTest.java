package com.anabneri.consumer;

import com.anabneri.entity.Book;
import com.anabneri.entity.BookStoreEvent;
import com.anabneri.entity.BookStoreEventType;
import com.anabneri.jpa.BookStoreEventRepository;
import com.anabneri.service.BookStoreEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;


@SpringBootTest
@EmbeddedKafka(topics = {"bookstore-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
, "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class BookStoreConsumerIntgTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    BookStoreEventsConsumer bookstoreEventsConsumer;

    @SpyBean
    BookStoreEventsService bookStoreEventsServiceSpy;

    @Autowired
    BookStoreEventRepository bookStoreEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        bookStoreEventRepository.deleteAll();
    }

    @Test
    void should_publish_a_bookStore_event_with_type_NEW() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"bookStoreEventId\":null,\"bookStoreEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"A culpa e das estrelas\",\"bookAuthor\":\"John Green\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(bookstoreEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(bookStoreEventsServiceSpy, times(1)).processBookStoreEvent(isA(ConsumerRecord.class));

        List<BookStoreEvent> libraryEventList = (List<BookStoreEvent>) bookStoreEventRepository.findAll();
        assert libraryEventList.size() ==1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getBookStoreEventId()!=null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });

    }

    @Test
    void should_publish_a_bookStore_event_with_type_UPDATE() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"bookStoreEventId\":null,\"bookStoreEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"A culpa e das estrelas\",\"bookAuthor\":\"John Green\"}}";
        BookStoreEvent libraryEvent = objectMapper.readValue(json, BookStoreEvent.class);
        libraryEvent.getBook().setBookStoreEvent(libraryEvent);
        bookStoreEventRepository.save(libraryEvent);

        Book updatedBook = Book.builder().
            bookId(456).bookName("Nova versao de Culpa é das estrelhas").bookAuthor("John Green").build();
        libraryEvent.setBookStoreEventType(BookStoreEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getBookStoreEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        BookStoreEvent persistedLibraryEvent = bookStoreEventRepository.findById(libraryEvent.getBookStoreEventId()).get();
        assertEquals("Nova versao de Culpa é das estrelhas", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void should_publish_a_modify_bookStore_event_with_a_not_valid_bookStoreeventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        Integer bookStoreEventId = 123;
        String json = "{\"bookStoreEventId\":" + bookStoreEventId  + ",\"bookStoreEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"A culpa e das estrelas new edition\",\"bookAuthor\":\"John Green\"}}";;
        System.out.println(json);
        kafkaTemplate.sendDefault(bookStoreEventId, json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(bookstoreEventsConsumer, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(bookStoreEventsServiceSpy, atLeast(1)).processBookStoreEvent(isA(ConsumerRecord.class));

        Optional<BookStoreEvent> libraryEventOptional = bookStoreEventRepository.findById(bookStoreEventId);
        assertFalse(libraryEventOptional.isPresent());
    }

    @Test
    void should_publish_a_modify_bookStore_event_with_a_null_bookStoreEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        Integer bookStoreEventId = null;
        String json = "{\"bookStoreEventId\":" + bookStoreEventId  + ",\"bookStoreEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"A culpa e das estrelas new edition\",\"bookAuthor\":\"John Green\"}}";;
        kafkaTemplate.sendDefault(bookStoreEventId, json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(bookstoreEventsConsumer, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(bookStoreEventsServiceSpy, atLeast(1)).processBookStoreEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        Integer bookStoreEventId = 000;
        String json = "{\"bookStoreEventId\":" + bookStoreEventId  + ",\"bookStoreEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"A culpa e das estrelas new edition\",\"bookAuthor\":\"John Green\"}}";;
        kafkaTemplate.sendDefault(bookStoreEventId, json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


//        verify(bookstoreEventsConsumer, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(bookStoreEventsServiceSpy, atLeast(1)).handleRecovery(isA(ConsumerRecord.class));
        verify(bookStoreEventsServiceSpy, atLeast(1)).processBookStoreEvent(isA(ConsumerRecord.class));
    }

}