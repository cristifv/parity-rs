package parity.rs.lifecycle;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import parity.rs.api.RequestListener;
import parity.rs.config.LifecycleProperties;
import parity.rs.queue.TenantQueues;

/**
 * Startup and graceful shutdown, signalled to the orchestrator with two empty files:
 * <ul>
 *   <li>the healthy file, created once the service has started;</li>
 *   <li>the drained file, created once the service has drained and can be stopped.</li>
 * </ul>
 * On {@code SIGTERM} this stops first (the highest phase):
 * <ol>
 *   <li>waits for its turn: one instance drains at a time ({@link InstanceCoordinator}), and
 *       this one keeps serving until then;</li>
 *   <li>removes the healthy file and pauses the request listener;</li>
 *   <li>waits until the tenant queues are empty;</li>
 *   <li>stops the listener, which commits the offsets and hands its partitions to the other
 *       instances;</li>
 *   <li>creates the drained file.</li>
 * </ol>
 * The wait and the drain share the drain timeout. If it runs out, there is no drained
 * file, and Kafka delivers the requests left again later.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class GracefulShutdown implements SmartLifecycle {

    private static final Duration PAUSE_WAIT = Duration.ofSeconds(5);

    private final KafkaListenerEndpointRegistry registry;
    private final TenantQueues queues;
    private final InstanceCoordinator coordinator;
    private final LifecycleProperties lifecycle;
    private volatile boolean running;

    /** Files left by an earlier run would tell the orchestrator the wrong thing. */
    @PostConstruct
    void removeStaleFiles() throws IOException {
        for (Path file : new Path[] {healthyFile(), drainedFile()}) {
            if (Files.deleteIfExists(file)) {
                log.info("removed {}, left by an earlier run", file);
            }
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void started() throws IOException {
        touch(healthyFile());
        coordinator.publish(InstanceEvent.STARTED);
        log.info("started; created {}", healthyFile());
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        long startedAt = System.nanoTime();
        long deadline = startedAt + lifecycle.drainTimeout().toNanos();
        try {
            if (coordinator.publish(InstanceEvent.DRAIN_REQUESTED) && !coordinator.awaitTurn(deadline)) {
                log.warn("still not our turn to drain; draining anyway");
            }
            Files.deleteIfExists(healthyFile());
            coordinator.publish(InstanceEvent.DRAINING);
            log.info("draining: removed {}, {} tenants with requests in progress", healthyFile(), queues.busyTenants());
            if (drain(deadline)) {
                touch(drainedFile());
                coordinator.publish(InstanceEvent.DRAINED);
                log.info("drained {} ms after the shutdown began; created {}",
                        (System.nanoTime() - startedAt) / 1_000_000, drainedFile());
            } else {
                log.error("not drained within {} ms: {} tenants still had requests, Kafka will deliver them again; no {}",
                        lifecycle.drainTimeout().toMillis(), queues.busyTenants(), drainedFile());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.error("graceful shutdown: {}", e.toString());
        } finally {
            coordinator.publish(InstanceEvent.STOPPED);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private boolean drain(long deadline) throws InterruptedException {
        MessageListenerContainer listener = registry.getListenerContainer(RequestListener.ID);
        if (listener == null || !listener.isRunning()) {
            return queues.awaitIdle(left(deadline));
        }
        // No new requests. The pause takes effect once the current poll is delivered.
        listener.pause();
        long pauseDeadline = Math.min(deadline, System.nanoTime() + PAUSE_WAIT.toNanos());
        while (!listener.isContainerPaused() && System.nanoTime() < pauseDeadline) {
            Thread.sleep(20);
        }
        boolean idle = queues.awaitIdle(left(deadline));
        // Commits the acknowledged offsets and leaves the consumer group.
        listener.stop();
        return idle;
    }

    private static Duration left(long deadline) {
        return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
    }

    private Path healthyFile() {
        return Path.of(lifecycle.healthyFile());
    }

    private Path drainedFile() {
        return Path.of(lifecycle.drainedFile());
    }

    private static void touch(Path file) throws IOException {
        Files.write(file, new byte[0]);
    }
}
