package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRepository;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public UserRecord findById(long id) {
        return userRepository.findById(id);
    }

    public void save(TransactionRecord transactionRecord) {
        transactionRepository.save(transactionRecord);
    }

    public List<UserRecord> findAllUsers() {
        return userRepository.findAll();
    }

    public List<TransactionRecord> findAllTransactions() {
        return transactionRepository.findAll();
    }
}
