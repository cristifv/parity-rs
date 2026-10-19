package parity.rs.queue;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import parity.rs.config.WorkersProperties;

class TenantQueuesTest {

    private TenantQueues queues;

    @AfterEach
    void stop() {
        queues.stop();
    }

    private void start(TenantQueues.JobProcessor processor) {
        queues = new TenantQueues(processor, new WorkersProperties(4, Duration.ofMillis(10)));
        queues.start();
    }

    private static TenantQueues.Job job(String payload, List<String> acked) {
        return new TenantQueues.Job(payload, () -> acked.add(payload));
    }

    @Test
    void oneTenantsRequestsRunInOrderOneAtATime() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        List<String> acked = new CopyOnWriteArrayList<>();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostAtOnce = new AtomicInteger();
        start(job -> {
            mostAtOnce.accumulateAndGet(running.incrementAndGet(), Math::max);
            seen.add(job.payload());
            running.decrementAndGet();
            job.ack().acknowledge();
        });

        for (int i = 0; i < 50; i++) {
            queues.submit("acme", job("acme-" + i, acked));
        }

        assertThat(queues.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(seen).containsExactlyElementsOf(acked).hasSize(50);
        assertThat(seen).isSortedAccordingTo((a, b) ->
                Integer.compare(Integer.parseInt(a.substring(5)), Integer.parseInt(b.substring(5))));
        assertThat(mostAtOnce).hasValue(1);
    }

    @Test
    void otherTenantsRunWhileOneIsBlocked() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch globexDone = new CountDownLatch(1);
        start(job -> {
            if (job.payload().equals("acme")) {
                await(release);
            } else {
                globexDone.countDown();
            }
        });

        queues.submit("acme", job("acme", new CopyOnWriteArrayList<>()));
        queues.submit("globex", job("globex", new CopyOnWriteArrayList<>()));

        assertThat(globexDone.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(queues.busyTenants()).isEqualTo(1);
        assertThat(queues.awaitIdle(Duration.ofMillis(100))).isFalse();
        release.countDown();
        assertThat(queues.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void aTenantLeavesTheMapWhenItsLastRequestHasRun() throws Exception {
        start(job -> { });
        queues.submit("acme", job("acme", new CopyOnWriteArrayList<>()));
        queues.submit("globex", job("globex", new CopyOnWriteArrayList<>()));
        assertThat(queues.awaitIdle(Duration.ofSeconds(5))).isTrue();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (queues.size() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(queues.size()).isZero();
    }

    @Test
    void aFailedRequestIsRetriedUntilItWorks() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<String> acked = new CopyOnWriteArrayList<>();
        List<String> after = new CopyOnWriteArrayList<>();
        start(job -> {
            if (job.payload().equals("set_route") && attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("database down");
            }
            if (job.payload().equals("get_route")) {
                after.add("ran after " + attempts.get() + " attempts");
            }
            job.ack().acknowledge();
        });

        queues.submit("acme", job("set_route", acked));
        queues.submit("acme", job("get_route", acked));

        assertThat(queues.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(acked).containsExactly("set_route", "get_route");
        assertThat(after).containsExactly("ran after 3 attempts");
    }

    @Test
    void requestsWithoutATenantShareOneQueue() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        List<String> seen = new CopyOnWriteArrayList<>();
        start(job -> {
            if (job.payload().equals("first")) {
                await(release);
            }
            seen.add(job.payload());
        });

        queues.submit(null, job("first", new CopyOnWriteArrayList<>()));
        queues.submit(null, job("second", new CopyOnWriteArrayList<>()));

        assertThat(queues.awaitIdle(Duration.ofMillis(100))).isFalse();
        assertThat(seen).isEmpty();
        release.countDown();
        assertThat(queues.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(seen).containsExactly("first", "second");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
