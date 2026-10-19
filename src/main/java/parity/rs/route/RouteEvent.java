package parity.rs.route;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Value;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

/** An event on the events topic, keyed by tenant id. */
@Value
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "at_ms", "tenant_id", "route", "previous"})
public class RouteEvent {
    public static final String ROUTE_CHANGED = "route.changed";
    public static final String ROUTE_DELETED = "route.deleted";

    String type;
    long atMs;
    String tenantId;
    RouteView route;
    /** The route before a change; absent for a new tenant and in {@code route.deleted}. */
    RouteView previous;
}
