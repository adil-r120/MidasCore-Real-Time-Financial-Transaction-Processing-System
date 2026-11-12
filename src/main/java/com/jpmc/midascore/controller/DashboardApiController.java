package com.jpmc.midascore.controller;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.service.TransactionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DashboardApiController {

    private final DatabaseConduit databaseConduit;
    private final TransactionService transactionService;

    public DashboardApiController(DatabaseConduit databaseConduit, TransactionService transactionService) {
        this.databaseConduit = databaseConduit;
        this.transactionService = transactionService;
    }

    @GetMapping("/users")
    public List<UserRecord> getUsers() {
        return databaseConduit.findAllUsers();
    }

    @GetMapping("/transactions/history")
    public List<TransactionRecord> getTransactionHistory() {
        List<TransactionRecord> list = databaseConduit.findAllTransactions();
        Collections.reverse(list);
        return list;
    }

    @PostMapping("/transactions")
    public ResponseEntity<Map<String, Object>> submitTransaction(@RequestBody Transaction transaction) {
        Map<String, Object> response = new HashMap<>();

        UserRecord sender = databaseConduit.findById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());

        if (sender == null) {
            response.put("success", false);
            response.put("message", "Validation Failed: Sender account ID " + transaction.getSenderId() + " does not exist.");
            return ResponseEntity.badRequest().body(response);
        }

        if (recipient == null) {
            response.put("success", false);
            response.put("message", "Validation Failed: Recipient account ID " + transaction.getRecipientId() + " does not exist.");
            return ResponseEntity.badRequest().body(response);
        }

        if (sender.getBalance() < transaction.getAmount()) {
            response.put("success", false);
            response.put("message", String.format("Solvency Check Failed: Sender %s has insufficient funds ($%.2f) for transaction amount ($%.2f).",
                    sender.getName(), sender.getBalance(), transaction.getAmount()));
            return ResponseEntity.badRequest().body(response);
        }

        transactionService.process(transaction);

        UserRecord updatedSender = databaseConduit.findById(transaction.getSenderId());
        UserRecord updatedRecipient = databaseConduit.findById(transaction.getRecipientId());

        response.put("success", true);
        response.put("message", "Transaction settled successfully.");
        response.put("senderNewBalance", updatedSender != null ? updatedSender.getBalance() : 0.0f);
        response.put("recipientNewBalance", updatedRecipient != null ? updatedRecipient.getBalance() : 0.0f);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/users/seed")
    public ResponseEntity<Map<String, Object>> seedAccounts() {
        Map<String, Object> response = new HashMap<>();
        List<UserRecord> existing = databaseConduit.findAllUsers();
        if (existing.isEmpty()) {
            databaseConduit.save(new UserRecord("bernie", 1200.23f));
            databaseConduit.save(new UserRecord("grommit", 2215.37f));
            databaseConduit.save(new UserRecord("maria", 2774.14f));
            databaseConduit.save(new UserRecord("mario", 12.34f));
            databaseConduit.save(new UserRecord("waldorf", 444.55f));
            databaseConduit.save(new UserRecord("whosit", 888.90f));
            databaseConduit.save(new UserRecord("whatsit", 777.60f));
            databaseConduit.save(new UserRecord("howsit", 68.70f));
            databaseConduit.save(new UserRecord("wilbur", 3476.21f));
            databaseConduit.save(new UserRecord("antonio", 2121.54f));
            databaseConduit.save(new UserRecord("calypso", 779421.33f));
            response.put("seeded", true);
            response.put("message", "Seeded 11 standard ledger accounts.");
        } else {
            response.put("seeded", false);
            response.put("message", "Accounts already exist (" + existing.size() + " accounts).");
        }
        response.put("users", databaseConduit.findAllUsers());
        return ResponseEntity.ok(response);
    }
}
