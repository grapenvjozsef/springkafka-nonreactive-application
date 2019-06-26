package com.grape.knowledgebase.srpingkafka.api;

import com.grape.knowledgebase.srpingkafka.entity.MessageDto;
import com.grape.knowledgebase.srpingkafka.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class BaseApi {

    @Autowired
    private MessageService kafkaProducerService;

    @PostMapping("/publish")
    public boolean publishToKafka(@RequestBody final MessageDto message) {
        kafkaProducerService.publish(message);
        return true;
    }
}
