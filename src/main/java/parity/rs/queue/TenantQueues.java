package parity.rs.queue;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import parity.rs.config.WorkersProperties;

/**
 * The concurrent queues: one queue per tenant, in a concurrent map keyed by tenant id. A
 * tenant's requests run one after another, in order. Different tenants run in parallel on
 * a pool of {@code workers.count} threads. Requests without a tenant share one queue.
 *
 * <p>A tenant's queue is the last request in its chain: a new request runs after it and
 * takes its place. The entry is removed once the tenant's last request has run.
 *
 * <p>A request that throws is retried after {@code workers.retry-delay} until it works,
 * holding up only its own tenant. Starts before the Kafka listener and stops after it.
 */
@Slf4j
@Component
public class TenantQueues implements SmartLifecycle {

    /** A request waiting in a queue, with the acknowledgment that commits its offset. */
    public record Job(String payload, Acknowledgment ack) {}

    /** Runs one job. Throws when the job should be retried. */
    @FunctionalInterface
    public interface JobProcessor {
        void process(Job job);
    }

    /** Below the Kafka listener containers' phase ({@code Integer.MAX_VALUE - 100}). */
    public static final int PHASE = Integer.MAX_VALUE - 200;

    /** The queue of requests that have no tenant. */
    static final String NO_TENANT = "";

    private final JobProcessor processor;
    private final WorkersProperties workers;
    private final ConcurrentMap<String, CompletableFuture<?>> queues = new ConcurrentHashMap<>();
    private ExecutorService executor;
    private volatile boolean running;

    public TenantQueues(JobProcessor processor, WorkersProperties workers) {
        this.processor = processor;
        this.workers = workers;
    }

    /** Queues the job after the tenant's earlier requests. */
    public void submit(String tenantId, Job job) {
        String key = tenantId == null ? NO_TENANT : tenantId;
        CompletableFuture<?> last = queues.compute(key, (tenant, previous) ->
                (previous == null ? CompletableFuture.completedFuture(null) : previous)
                        .handleAsync((result, error) -> runUntilDone(job), executor));
        last.whenComplete((result, error) -> queues.remove(key, last));
    }

    /** Waits until every queued request has run. False if the timeout ran out first. */
    public boolean awaitIdle(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            CompletableFuture<?>[] busy = queues.values().stream()
                    .filter(last -> !last.isDone())
                    .toArray(CompletableFuture[]::new);
            if (busy.length == 0) {
                return true;
            }
            long left = deadline - System.nanoTime();
            if (left <= 0) {
                return false;
            }
            try {
                CompletableFuture.allOf(busy).get(left, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                return false;
            } catch (ExecutionException e) {
                // Jobs don't throw; look again.
            }
        }
    }

    /** Tenants with requests queued or running. */
    public int busyTenants() {
        return (int) queues.values().stream().filter(last -> !last.isDone()).count();
    }

    /** Tenants in the map, including ones whose last request has just finished. */
    int size() {
        return queues.size();
    }

    @Override
    public void start() {
        executor = Executors.newFixedThreadPool(workers.count(), Thread.ofPlatform().name("worker-", 0).factory());
        running = true;
        log.info("tenant queues started, {} worker threads", workers.count());
    }

    /**
     * Stops the workers. By now the graceful shutdown has drained the queues; a request
     * still running is abandoned without its acknowledgment, so Kafka delivers it again.
     */
    @Override
    public void stop() {
        running = false;
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    /** Never throws, so a failed request can't stop the requests queued after it. */
    private Void runUntilDone(Job job) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processor.process(job);
                return null;
            } catch (RuntimeException e) {
                log.warn("request failed, retrying in {} ms: {}", workers.retryDelay().toMillis(), e.toString());
                try {
                    Thread.sleep(workers.retryDelay());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return null;
    }
}
