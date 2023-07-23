package com.colossus.twittertokafkaservice;

import com.colossus.twittertokafkaservice.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@RequiredArgsConstructor
@ComponentScan(basePackages = "com.colossus")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private final StreamRunner streamRunner;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        streamRunner.start();
    }
}
