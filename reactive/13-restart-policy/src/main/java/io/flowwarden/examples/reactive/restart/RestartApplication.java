package io.flowwarden.examples.reactive.restart;

import io.flowwarden.stream.annotation.EnableFlowWarden;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Sample 13 — {@code @RestartPolicy}, reactive twin of
 * {@code imperative/13-restart-policy}.
 *
 * <p>{@link CollectionDropper} periodically drops the watched collection;
 * the reactive pipeline completes on the resulting {@code invalidate},
 * FlowWarden reports the stream {@code CRASHED} and resubscribes it on the
 * cadence declared by {@code @RestartPolicy}. {@link RestartObserver}
 * makes every step visible in the logs.</p>
 */
@SpringBootApplication(scanBasePackages = "io.flowwarden.examples")
@EnableFlowWarden
@EnableScheduling
public class RestartApplication {

    public static void main(String[] args) {
        SpringApplication.run(RestartApplication.class, args);
    }
}
