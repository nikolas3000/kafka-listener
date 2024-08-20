package com.example.demo.service;


import com.example.demo.model.MessageRecord;
import com.example.demo.repositories.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KafkaMessageListener {
    private final MessageRecordRepository messageRecordRepository;

    public KafkaMessageListener(MessageRecordRepository messageRecordRepository) {
        this.messageRecordRepository = messageRecordRepository;
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

            System.out.println("Записано в БД: msgId=" + msgId + ", timeRq=" + timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}