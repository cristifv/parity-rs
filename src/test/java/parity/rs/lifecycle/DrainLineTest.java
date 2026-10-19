package parity.rs.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DrainLineTest {

    private static final long TIMEOUT = 30_000;

    private final DrainLine line = new DrainLine();

    private static InstanceEvent event(String type, String instance, long atMs) {
        return InstanceEvent.builder().type(type).service("parity-rs").instance(instance)
                .atMs(atMs).drainTimeoutMs(TIMEOUT).build();
    }

    @Test
    void instancesDrainInTheOrderTheyAsked() {
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "b", 1000));
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "a", 1001));
        assertThat(line.first(2000)).isEqualTo("b");

        line.apply(event(InstanceEvent.DRAINING, "b", 1500));
        assertThat(line.first(2000)).isEqualTo("b");

        line.apply(event(InstanceEvent.DRAINED, "b", 3000));
        assertThat(line.first(3000)).isEqualTo("a");

        line.apply(event(InstanceEvent.STOPPED, "a", 4000));
        assertThat(line.first(4000)).isNull();
    }

    @Test
    void aSecondRequestKeepsItsPlace() {
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "a", 1000));
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "b", 1001));
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "a", 1002));
        assertThat(line.first(2000)).isEqualTo("a");
    }

    @Test
    void stoppedWithoutDrainingLeavesTheLine() {
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "a", 1000));
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "b", 1001));
        line.apply(event(InstanceEvent.STOPPED, "a", 2000));
        assertThat(line.first(2000)).isEqualTo("b");
    }

    @Test
    void aKilledInstanceGoesStaleAfterItsDrainTimeout() {
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "killed", 1000));
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "b", 20_000));
        long stillWaiting = 1000 + TIMEOUT + DrainLine.STALE_MARGIN_MS;
        assertThat(line.first(stillWaiting)).isEqualTo("killed");
        assertThat(line.first(stillWaiting + 1)).isEqualTo("b");
        assertThat(line.contains("killed")).isTrue();
    }

    @Test
    void aRestartedInstanceIsNoLongerWaiting() {
        line.apply(event(InstanceEvent.DRAIN_REQUESTED, "a", 1000));
        line.apply(event(InstanceEvent.STARTED, "a", 2000));
        assertThat(line.contains("a")).isFalse();
        assertThat(line.first(2000)).isNull();
    }
}
