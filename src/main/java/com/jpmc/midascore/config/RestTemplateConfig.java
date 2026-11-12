package com.jpmc.midascore.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate(
            RestTemplateBuilder builder,
            @Value("${services.incentive-api.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${services.incentive-api.read-timeout-ms:5000}") long readTimeoutMs) {
        return builder
                .setConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .setReadTimeout(Duration.ofMillis(readTimeoutMs))
                .build();
    }
}
