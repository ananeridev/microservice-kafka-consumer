package com.anabneri.config;

import com.anabneri.service.BookStoreEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class BookStoreEventsConsumerConfig {

    @Autowired BookStoreEventsService bookStoreEventsService;

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);

        factory.setConcurrency(3);
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Excpetion in consumerConfig is {} and the record is {}" + thrownException.getMessage(), data);
            //persistir o dado aqui
        }));
        factory.setRetryTemplate(retryTemplate());
        factory.setRecoveryCallback(context -> {
            if(context.getLastThrowable().getCause() instanceof RecoverableDataAccessException){
                //invoke recovery logic
                log.info("Inside the recoverable logic");
                Arrays.asList(context.attributeNames())
                    .forEach(attributeName -> {
                        log.info("Attribute name is : {} ", attributeName);
                        log.info("Attribute Value is : {} ", context.getAttribute(attributeName));
                    });

                // aqui vai aparecer o ehxauted
                ConsumerRecord<Integer, String> consumerRecord =
                    (ConsumerRecord<Integer, String>) context.getAttribute("record");
                bookStoreEventsService.handleRecovery(consumerRecord);
            }else{
                log.info("Inside the non recoverable logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }

            return null;
        });
        return factory;
    }

    private RetryTemplate retryTemplate() {

        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();

        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
//        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//        simpleRetryPolicy.setMaxAttempts(3);
     Map<Class<? extends Throwable>, Boolean> excpetionMapper = new HashMap<>();
        excpetionMapper.put(IllegalArgumentException.class, false);
        excpetionMapper.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, excpetionMapper, true );
        return simpleRetryPolicy;
    }

}
