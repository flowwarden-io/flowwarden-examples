package io.flowwarden.examples.imperative.restart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The fault injector: drops the watched collection on a fixed cadence.
 *
 * <p>Dropping a watched collection makes MongoDB deliver an {@code invalidate}
 * event and close the cursor — the same shape of runtime death as a
 * primary stepdown or a non-resumable server error, but deterministic and
 * reproducible from inside the JVM. The data generator keeps inserting, so
 * the collection is recreated on the next insert and the resubscribed
 * stream has events to process again.</p>
 */
@Component
public class CollectionDropper {

    private static final Logger log = LoggerFactory.getLogger(CollectionDropper.class);
    private static final String COLLECTION = "orders-restart";

    private final MongoTemplate mongoTemplate;

    public CollectionDropper(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Scheduled(initialDelayString = "${examples.restart-demo.initial-delay-ms:10000}",
               fixedDelayString = "${examples.restart-demo.drop-interval-ms:20000}")
    void dropWatchedCollection() {
        log.warn("[restart-demo] dropping collection '{}' — the change stream cursor will be invalidated", COLLECTION);
        mongoTemplate.dropCollection(COLLECTION);
    }
}
