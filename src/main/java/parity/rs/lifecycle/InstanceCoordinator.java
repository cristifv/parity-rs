package parity.rs.lifecycle;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import parity.rs.config.LifecycleProperties;
import parity.rs.config.TopicsProperties;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Coordinates graceful shutdown between instances, so that one instance drains at a time
 * and the others keep serving and take over its partitions. Each instance publishes its
 * lifecycle events to partition 0 of the instances topic and reads them all back into the
 * {@link DrainLine}.
 *
 * <p>Best effort: if the events can't be written or read back, an instance drains
 * without waiting.
 */
@Slf4j
@Component
public class InstanceCoordinator implements SmartLifecycle {

    /** How long to wait for our own drain request to come back from Kafka. */
    private static final Duration OWN_REQUEST_WAIT = Duration.ofSeconds(5);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaTemplate<String, String> kafka;
    private final ConsumerFactory<?, ?> consumerFactory;
    private final JsonMapper json;
    private final TopicsProperties topics;
    private final LifecycleProperties lifecycle;
    private final Clock clock;
    private final String service;
    @Getter
    private final String instanceId;

    private final DrainLine line = new DrainLine(); // guarded by this
    private KafkaConsumer<String, String> consumer;
    private Thread monitor;
    private volatile boolean running;

    public InstanceCoordinator(KafkaTemplate<String, String> kafka, ConsumerFactory<?, ?> consumerFactory,
                               JsonMapper json, TopicsProperties topics, LifecycleProperties lifecycle,
                               Clock clock, @Value("${spring.application.name}") String service) {
        this.kafka = kafka;
        this.consumerFactory = consumerFactory;
        this.json = json;
        this.topics = topics;
        this.lifecycle = lifecycle;
        this.clock = clock;
        this.service = service;
        this.instanceId = StringUtils.hasText(lifecycle.instanceId()) ? lifecycle.instanceId() : defaultInstanceId();
    }

    /** Publishes one of this instance's lifecycle events. False if Kafka didn't take it. */
    public boolean publish(String type) {
        InstanceEvent event = InstanceEvent.builder()
                .type(type)
                .service(service)
                .instance(instanceId)
                .atMs(clock.millis())
                .drainTimeoutMs(lifecycle.drainTimeout().toMillis())
                .build();
        try {
            kafka.send(topics.instances(), 0, instanceId, json.writeValueAsString(event))
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.warn("couldn't publish {}: {}", type, e.toString());
            return false;
        }
    }

    /**
     * Waits until this instance is first in the drain line. Returns false if the deadline
     * passes first. Returns true at once if our own drain request doesn't come back from
     * Kafka within a few seconds, since then nothing can be coordinated.
     */
    public boolean awaitTurn(long deadlineNanos) throws InterruptedException {
        long ownRequestDeadline = Math.min(deadlineNanos, System.nanoTime() + OWN_REQUEST_WAIT.toNanos());
        String waitingFor = null;
        synchronized (this) {
            while (true) {
                if (line.contains(instanceId)) {
                    String first = line.first(clock.millis());
                    if (instanceId.equals(first)) {
                        return true;
                    }
                    if (first != null && !first.equals(waitingFor)) {
                        waitingFor = first;
                        log.info("waiting for {} to drain first", first);
                    }
                } else if (System.nanoTime() > ownRequestDeadline) {
                    log.warn("our drain request didn't come back from {}; draining without waiting", topics.instances());
                    return true;
                }
                long left = deadlineNanos - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, Math.min(left, TimeUnit.MILLISECONDS.toNanos(200)));
            }
        }
    }

    @Override
    public void start() {
        Map<String, Object> props = new HashMap<>(consumerFactory.getConfigurationProperties());
        props.remove(ConsumerConfig.GROUP_ID_CONFIG); // assigned, not subscribed: no group, no commits
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        running = true;
        monitor = new Thread(this::monitor, "instances");
        monitor.start();
        log.info("instance {}", instanceId);
    }

    @Override
    public void stop() {
        running = false;
        consumer.wakeup();
        try {
            monitor.join(Duration.ofSeconds(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Starts before and stops after {@link GracefulShutdown}. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 50;
    }

    private void monitor() {
        TopicPartition partition = new TopicPartition(topics.instances(), 0);
        try {
            consumer.assign(List.of(partition));
            // Requests older than two drain timeouts are stale, so there's no need to read further back.
            long from = clock.millis() - 2 * lifecycle.drainTimeout().toMillis() - DrainLine.STALE_MARGIN_MS;
            OffsetAndTimestamp offset = consumer.offsetsForTimes(Map.of(partition, from)).get(partition);
            if (offset != null) {
                consumer.seek(partition, offset.offset());
            } else {
                consumer.seekToEnd(List.of(partition));
            }
            while (running) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    apply(record.value());
                }
            }
        } catch (WakeupException e) {
            // Stopping.
        } catch (RuntimeException e) {
            log.error("reading {} stopped: {}", topics.instances(), e.toString());
        } finally {
            consumer.close();
        }
    }

    private void apply(String value) {
        InstanceEvent event;
        try {
            event = InstanceEvent.from(json.readTree(value));
        } catch (JacksonException e) {
            log.warn("ignored an invalid instance event: {}", value);
            return;
        }
        if (!service.equals(event.getService())) {
            return;
        }
        synchronized (this) {
            line.apply(event);
            notifyAll();
        }
    }

    private static String defaultInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        return host + "-" + ProcessHandle.current().pid();
    }
}
