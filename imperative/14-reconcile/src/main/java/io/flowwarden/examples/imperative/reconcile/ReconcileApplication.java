package io.flowwarden.examples.imperative.reconcile;

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
@EnableScheduling
public class ReconcileApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReconcileApplication.class, args);
    }
}
