package io.flowwarden.examples.imperative.reconcile;

import io.flowwarden.examples.common.model.Order;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * The <em>derived view</em> this sample maintains from the stream: an
 * in-memory index of orders, keyed by id. A real application would hold a
 * search engine, a cache or a cross-collection projection here — anything
 * that is only correct if it has seen every event.
 *
 * <p>Over a lost history window the index is wrong: orders inserted during
 * the downtime were never delivered. {@link #resyncCreatedSince} is the
 * repair the {@code @Reconcile} hook runs before the stream resumes.</p>
 */
@Component
public class OrderSearchIndex {

    static final String COLLECTION = "orders-reconcile";

    private static final Logger log = LoggerFactory.getLogger(OrderSearchIndex.class);

    private final MongoTemplate mongoTemplate;
    private final Map<String, Order> byId = new ConcurrentHashMap<>();
    private final AtomicLong indexedByStream = new AtomicLong();
    private final List<Integer> resyncs = new CopyOnWriteArrayList<>();

    public OrderSearchIndex(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /** Indexes one order; {@code source} is {@code "stream"} or {@code "resync"} (logs only). */
    public void index(Order order, String source) {
        byId.put(order.getId(), order);
        if ("stream".equals(source)) {
            long n = indexedByStream.incrementAndGet();
            log.info("[reconcile] #{} indexed order {} ({}, total={})",
                    n, order.getId().substring(0, 8), order.getStatus(), order.getTotal());
        }
    }

    /**
     * Re-synchronises the index from the collection: every order created
     * since {@code since} is (re)indexed. Idempotent — indexing an order
     * already present only overwrites it with the same data. Returns the
     * number of orders re-synced.
     */
    public int resyncCreatedSince(Instant since) {
        List<Order> orders = mongoTemplate.find(query(where("createdAt").gte(since)), Order.class, COLLECTION);
        orders.forEach(order -> index(order, "resync"));
        resyncs.add(orders.size());
        return orders.size();
    }

    public int size() {
        return byId.size();
    }

    public long getIndexedByStream() {
        return indexedByStream.get();
    }

    /** The size of every re-sync performed so far, in order. */
    public List<Integer> getResyncs() {
        return List.copyOf(resyncs);
    }
}
