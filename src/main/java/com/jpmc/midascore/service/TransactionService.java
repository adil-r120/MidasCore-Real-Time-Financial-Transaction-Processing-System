package com.jpmc.midascore.service;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionService {
    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);

    private final DatabaseConduit databaseConduit;
    private final IncentiveClient incentiveClient;

    public TransactionService(DatabaseConduit databaseConduit, IncentiveClient incentiveClient) {
        this.databaseConduit = databaseConduit;
        this.incentiveClient = incentiveClient;
    }

    public void process(Transaction transaction) {
        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            // Outbound network call executed outside the DB transaction to prevent connection pool starvation
            float incentiveAmount = incentiveClient.fetchIncentive(transaction);

            // Execute atomic ledger debit, credit, and audit record creation within @Transactional boundary
            settleTransaction(transaction.getSenderId(), transaction.getRecipientId(), transaction.getAmount(), incentiveAmount);
        } else {
            logger.warn("Transaction failed validation: {}", transaction);
        }
    }

    @Transactional
    public void settleTransaction(long senderId, long recipientId, float amount, float incentiveAmount) {
        UserRecord sender = databaseConduit.findById(senderId);
        UserRecord recipient = databaseConduit.findById(recipientId);

        if (sender != null && recipient != null && sender.getBalance() >= amount) {
            sender.setBalance(sender.getBalance() - amount);
            recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount, incentiveAmount);
            databaseConduit.save(transactionRecord);

            logger.info("Transaction processed successfully: sender {} new balance {}, recipient {} new balance {} (incentive: {})",
                    sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance(), incentiveAmount);
        } else {
            logger.warn("Transaction failed validation during settlement: senderId={}, amount={}", senderId, amount);
        }
    }
}

