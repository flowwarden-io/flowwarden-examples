package io.flowwarden.examples.imperative.reconcile;

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
        "examples.reconcile-demo.initial-delay-ms=5000",
        "examples.reconcile-demo.downtime-ms=2000",
        "examples.reconcile-demo.interval-ms=5000"
})
@Testcontainers
class ReconcileSmokeTest {

    @Container
    @ServiceConnection
    static MongoDBContainer mongo = new MongoDBContainer("mongo:6.0");

    @Autowired
    OrderSearchIndex index;

    @Autowired
    ReconcileObserver observer;

    @Test
    void theIndexIsReconciledBeforeTheStreamResumesAfterAHistoryLoss() {
        // The stream feeds the index before the first injected loss.
        await().atMost(60, SECONDS).until(() -> index.getIndexedByStream() > 0);

        // The injected loss reached cascade level 3 and the hook ran — once,
        // before the stream resumed.
        await().atMost(60, SECONDS).until(() -> observer.getHistoryLosts() >= 1);
        await().atMost(30, SECONDS).until(() -> !observer.getReconciledAttempts().isEmpty());
        assertThat(observer.getReconciledAttempts().get(0)).as("first hook invocation").isEqualTo(1);
        assertThat(observer.getReconciliationFailures()).isZero();

        // Orders inserted during the downtime were never delivered: the
        // re-sync is what put them in the index.
        assertThat(index.getResyncs()).isNotEmpty();
        assertThat(index.getResyncs().get(0)).as("orders re-synced over the lost window").isGreaterThanOrEqualTo(1);

        // And the stream feeds the index again afterwards.
        long before = index.getIndexedByStream();
        await().atMost(30, SECONDS).until(() -> index.getIndexedByStream() > before);
    }
}
