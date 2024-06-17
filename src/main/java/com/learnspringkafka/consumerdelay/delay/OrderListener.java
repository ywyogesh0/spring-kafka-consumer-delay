package com.learnspringkafka.consumerdelay.delay;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
@Slf4j
public class OrderListener {

    private final ObjectMapper objectMapper;

    public OrderListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "1", include = KafkaBackoffException.class, dltStrategy = DltStrategy.NO_DLT)
    @KafkaListener(topics = {"orders-2"}, groupId = "orders-2-group")
    public void handleOrders(String order) throws JsonProcessingException {
        Order orderDetails = objectMapper.readValue(order, Order.class);
        log.info("Order: {}", orderDetails);
    }
}
