package com.learnspringkafka.consumerdelay.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnspringkafka.consumerdelay.delay.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
public class OrderProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) {
        Properties propsMap = new Properties();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(propsMap)) {

            Order order1 = Order.builder()
                    .orderId(UUID.randomUUID())
                    .price(1.0)
                    .orderGeneratedDateTime(LocalDateTime.now())
                    .address(List.of("1"))
                    .build();

            Order order2 = Order.builder()
                    .orderId(UUID.randomUUID())
                    .price(1.0)
                    .orderGeneratedDateTime(LocalDateTime.now())
                    .address(List.of("2"))
                    .build();

            Order order3 = Order.builder()
                    .orderId(UUID.randomUUID())
                    .price(1.0)
                    .orderGeneratedDateTime(LocalDateTime.now())
                    .address(List.of("3"))
                    .build();

            Order order4 = Order.builder()
                    .orderId(UUID.randomUUID())
                    .price(1.0)
                    .orderGeneratedDateTime(LocalDateTime.now())
                    .address(List.of("4"))
                    .build();

            Order order5 = Order.builder()
                    .orderId(UUID.randomUUID())
                    .price(1.0)
                    .orderGeneratedDateTime(LocalDateTime.now())
                    .address(List.of("5"))
                    .build();

            List<Order> orderList = Arrays.asList(order1, order2, order3, order4, order5);
            orderList
                    .forEach(
                            order -> {
                                String orderString;
                                try {
                                    orderString = objectMapper.writeValueAsString(order);
                                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("orders-2", orderString);
                                    // send kafka records
                                    try {
                                        log.info("producerRecord : {}", producerRecord);
                                        RecordMetadata recordMetadata = producer.send(producerRecord).get();

                                        log.info("RecordMetadata: {}", recordMetadata);
                                    } catch (Exception e) {
                                        log.error("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
                                    }
                                } catch (JsonProcessingException e) {
                                    log.error("ERROR: JsonProcessingException -> {}", e.getMessage(), e);
                                }

                                try {
                                    log.info("Sleeping for 5 seconds...");
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    log.error("ERROR: InterruptedException -> {}", e.getMessage(), e);
                                }
                            }
                    );
        }
    }
}
