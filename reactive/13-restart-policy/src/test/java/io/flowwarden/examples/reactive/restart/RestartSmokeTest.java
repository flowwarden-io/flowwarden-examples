package io.flowwarden.examples.reactive.restart;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = {
        "examples.restart-demo.initial-delay-ms=5000",
        "examples.restart-demo.drop-interval-ms=5000"
})
@Testcontainers
class RestartSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    RestartHandler handler;

    @Autowired
    RestartObserver observer;

    @Test
    void streamIsResubscribedAfterTheCollectionIsDropped() {
        // The stream flows before the first drop.
        await().atMost(60, SECONDS).until(() -> handler.getProcessed() > 0);

        // The drop reached the cursor and the stream died.
        await().atMost(60, SECONDS).until(() -> observer.getInvalidations() >= 1);
        await().atMost(30, SECONDS).until(() -> observer.getCrashes() >= 1);

        // Only the restart loop can produce this signal: the stream was
        // resubscribed by FlowWarden, not by anything in this test.
        await().atMost(60, SECONDS).until(() -> !observer.getRestartAttempts().isEmpty());
        assertThat(observer.getRestartAttempts().get(0))
                .as("resubscription attempt number")
                .isGreaterThanOrEqualTo(1);

        // And it processes events again — the generator recreated the
        // collection, the resubscribed cursor sees the new inserts.
        long before = handler.getProcessed();
        await().atMost(30, SECONDS).until(() -> handler.getProcessed() > before);
    }
}
