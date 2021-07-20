package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;
import scala.Int;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer,String> kafkaTemplate;

    //spy instance retains the features of parent class while letting us stub some features
    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer eventProducer;

    @Test
    void sendLibraryEvent_Approach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        //builder pattern over static reference of Book class
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Rajat")
                .bookName("Kafka Practice")
                .build();

        //builder pattern over static reference of LibraryEvent class
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        //To be able to set value to future result
        SettableListenableFuture future = new SettableListenableFuture();

        //expect exception form the future result
        future.setException(new RuntimeException("Exception Calling Kafka"));

        //mock the method that will be invoked and return the expected result.
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        //send method form kafkaTemplate will throw exception when returned, as expected.
        assertThrows(Exception.class, ()->eventProducer.sendLibraryEvent_Approach2(libraryEvent).get());

    }

    @Test
    void sendLibraryEvent_Approach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given
        //builder pattern over static reference of Book class
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka using Spring Boot")
                .build();

        //builder pattern over static reference of LibraryEvent class
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        //Method that can be used to serialize any Java value as String output.
        String record = objectMapper.writeValueAsString(libraryEvent);

        //implementation of ListenableFuture in addition to set(object) and setException methods.Instance is mutable
        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),record );

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);

        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);

        //set instance to the future
        future.set(sendResult);

        //when send method of kafkaTemplate is invoked using producer record
        when(kafkaTemplate.send(isA(ProducerRecord.class)))
                //then return listenable future
                .thenReturn(future);

        //mock will be invoked within the below method call
        ListenableFuture<SendResult<Integer,String>> listenableFuture =  eventProducer.sendLibraryEvent_Approach2(libraryEvent);

        // get the return value from listenable future for assertion
        SendResult<Integer,String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition()==1;

    }
}
