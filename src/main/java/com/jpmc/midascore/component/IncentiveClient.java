package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveClient {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveClient.class);

    private final RestTemplate restTemplate;
    private final String incentiveApiUrl;

    public IncentiveClient(
            RestTemplate restTemplate,
            @Value("${services.incentive-api.url:http://localhost:8080/incentive}") String incentiveApiUrl) {
        this.restTemplate = restTemplate;
        this.incentiveApiUrl = incentiveApiUrl;
    }

    public float fetchIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
            if (incentive != null) {
                return incentive.getAmount();
            }
        } catch (Exception e) {
            logger.error("Failed to query Incentive API at {}, applying 0 incentive fallback", incentiveApiUrl, e);
        }
        return 0.0f;
    }
}

