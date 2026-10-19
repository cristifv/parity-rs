package parity.rs.lifecycle;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Value;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

/** A lifecycle event of one instance, on the instances topic. */
@Value
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonPropertyOrder({"type", "service", "instance", "at_ms", "drain_timeout_ms"})
public class InstanceEvent {
    public static final String STARTED = "instance.started";
    public static final String DRAIN_REQUESTED = "instance.drain_requested";
    public static final String DRAINING = "instance.draining";
    public static final String DRAINED = "instance.drained";
    public static final String STOPPED = "instance.stopped";

    String type;
    String service;
    String instance;
    long atMs;
    long drainTimeoutMs;

    static InstanceEvent from(JsonNode node) {
        return InstanceEvent.builder()
                .type(node.path("type").asString())
                .service(node.path("service").asString())
                .instance(node.path("instance").asString())
                .atMs(node.path("at_ms").asLong())
                .drainTimeoutMs(node.path("drain_timeout_ms").asLong())
                .build();
    }
}
