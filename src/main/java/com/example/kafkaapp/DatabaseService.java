package com.example.kafkaapp;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DatabaseService {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Insert initial state: Both Main record and Audit log inserted in a single transaction
    @Transactional
    public void insertInitialState(String transactionId, String payload) {
        String sql1 = "INSERT INTO TRANSACTION_RECORD (TXN_ID, PAYLOAD, STATUS) VALUES (?, ?, 'PENDING')";
        jdbcTemplate.update(sql1, transactionId, payload);

        String sql2 = "INSERT INTO AUDIT_LOG (TXN_ID, ACTION_TIME, ACTION) VALUES (?, CURRENT_TIMESTAMP, 'STARTED_PROCESSING')";
        jdbcTemplate.update(sql2, transactionId);
    }

    // Insert 2: Audit log (standalone, for logging individual events)
    public void insertAuditLog(String transactionId, String action) {
        String sql = "INSERT INTO AUDIT_LOG (TXN_ID, ACTION_TIME, ACTION) VALUES (?, CURRENT_TIMESTAMP, ?)";
        jdbcTemplate.update(sql, transactionId, action);
    }

    // Update flag to F if SOAP call fails, and add an audit log
    @Transactional
    public void markAsFailed(String transactionId, String errorMessage) {
        String sql1 = "UPDATE TRANSACTION_RECORD SET STATUS = 'F' WHERE TXN_ID = ?";
        jdbcTemplate.update(sql1, transactionId);

        String sql2 = "INSERT INTO AUDIT_LOG (TXN_ID, ACTION_TIME, ACTION) VALUES (?, CURRENT_TIMESTAMP, ?)";
        jdbcTemplate.update(sql2, transactionId, "FAILED_WITH_ERROR: " + errorMessage);
    }

    // Mark as Success if SOAP call completes successfully, and add an audit log
    @Transactional
    public void markAsSuccess(String transactionId) {
        String sql1 = "UPDATE TRANSACTION_RECORD SET STATUS = 'S' WHERE TXN_ID = ?";
        jdbcTemplate.update(sql1, transactionId);

        String sql2 = "INSERT INTO AUDIT_LOG (TXN_ID, ACTION_TIME, ACTION) VALUES (?, CURRENT_TIMESTAMP, 'COMPLETED_SUCCESSFULLY')";
        jdbcTemplate.update(sql2, transactionId);
    }
}
