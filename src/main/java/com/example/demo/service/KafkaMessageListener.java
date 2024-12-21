package com.example.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.demo.model.MessageRecord;
import com.example.demo.repositories.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KafkaMessageListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);
    private final MessageRecordRepository messageRecordRepository;

    public KafkaMessageListener(MessageRecordRepository messageRecordRepository) {
        this.messageRecordRepository = messageRecordRepository;
    }
    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void justRead (String message){
        System.out.println("Получено сообщение: " +message);
        log.info("Получено сообщение: {}", message);
    }

    @KafkaListener(topics = "kafka-topic-test", groupId = "my-group")
    public void listen(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(message);

            String msgId = jsonNode.get("msg_id").asText();
            String timestamp = jsonNode.get("timestamp").asText();

            System.out.println(msgId);

            MessageRecord messageRecord = new MessageRecord();
            messageRecord.setMsgId(msgId);
            messageRecord.setTimeRq(timestamp);
            messageRecordRepository.save(messageRecord);

            log.info("Записано в БД: msgId={}, timeRq={}", msgId, timestamp);
        } catch (Exception e) {
            log.error("Ошибка при обработке сообщения: {}", e.getMessage(), e);
        }
    }
}