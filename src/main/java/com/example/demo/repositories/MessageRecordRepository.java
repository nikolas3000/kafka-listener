package com.example.demo.repositories;



import com.example.demo.model.MessageRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageRecordRepository extends JpaRepository<MessageRecord, Long> {
}