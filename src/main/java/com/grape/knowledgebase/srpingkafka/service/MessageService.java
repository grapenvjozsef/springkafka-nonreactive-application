package com.grape.knowledgebase.srpingkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.grape.knowledgebase.srpingkafka.entity.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String kafkaTopic;
    private final ObjectMapper objectMapper;

    public MessageService(final KafkaTemplate<String, String> kafkaTemplate,
                          @Value("${kafka.topic}") final String kafkaTopic,
                          final ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTopic = kafkaTopic;
        this.objectMapper = objectMapper;
    }

    /** Üzenet küldése */
    public boolean publish(final MessageDto messageDto) {
        log.debug("Publishing message ....Topic.: " + kafkaTopic);
        try {
            kafkaTemplate.send(kafkaTopic, objectMapper.writeValueAsString(messageDto));
            log.info("Published events ...");
        } catch (JsonProcessingException e) {
            log.error("Couldn't parse messageDto to string. Error.: " + e.getLocalizedMessage());
            return false;
        }

        return true;
    }

    /** Elküldött üzenet kiolvasása és visszigazolása */
    @KafkaListener(topics="#{'${kafka.topic}'}", containerFactory = "kafkaListenerContainerFactory")
    public void processMessage(@Payload final String payload, final Acknowledgment acknowledgment) {
        log.info("Received content = " + payload);
        acknowledgment.acknowledge();
    }
}
