package io.flowwarden.examples.imperative.restart;

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Sample 13 — {@code @RestartPolicy}.
 *
 * <p>Shows the managed restart loop at work: {@link CollectionDropper}
 * periodically drops the watched collection, which invalidates the change
 * stream cursor; FlowWarden reports the stream {@code CRASHED}, then
 * resubscribes it on the cadence declared by {@code @RestartPolicy}.
 * {@link RestartObserver} makes every step visible in the logs.</p>
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
@EnableScheduling
public class RestartApplication {

    public static void main(String[] args) {
        SpringApplication.run(RestartApplication.class, args);
    }
}
