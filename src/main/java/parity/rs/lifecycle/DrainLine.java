package parity.rs.lifecycle;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The instances waiting to drain, in the order of their {@code instance.drain_requested}
 * events. Every instance reads the same single-partition topic, so all of them agree on
 * the order. An instance leaves the line with its {@code drained}, {@code stopped} or
 * {@code started} event, or once its request is older than its drain timeout: by then it
 * has finished or was killed. Not thread-safe.
 */
final class DrainLine {

    /** Allowance for clock differences between hosts. */
    static final long STALE_MARGIN_MS = 5_000;

    private record Entry(long requestedAtMs, long drainTimeoutMs) {
        boolean stale(long nowMs) {
            return nowMs > requestedAtMs + drainTimeoutMs + STALE_MARGIN_MS;
        }
    }

    private final Map<String, Entry> line = new LinkedHashMap<>();

    void apply(InstanceEvent event) {
        switch (event.getType()) {
            case InstanceEvent.DRAIN_REQUESTED ->
                    line.putIfAbsent(event.getInstance(), new Entry(event.getAtMs(), event.getDrainTimeoutMs()));
            case InstanceEvent.STARTED, InstanceEvent.DRAINED, InstanceEvent.STOPPED -> line.remove(event.getInstance());
            default -> {
                // instance.draining only informs.
            }
        }
    }

    boolean contains(String instance) {
        return line.containsKey(instance);
    }

    /** The instance whose turn it is to drain, or null if nobody is waiting. */
    String first(long nowMs) {
        for (Map.Entry<String, Entry> entry : line.entrySet()) {
            if (!entry.getValue().stale(nowMs)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
