package parity.rs.route;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Value;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

/** A tenant route as it appears in responses and events. */
@Value
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonPropertyOrder({"tenant_id", "home_zone", "zone", "cell", "deployment_unit", "version", "updated_at_ms"})
public class RouteView {
    String tenantId;
    String homeZone;
    String zone;
    String cell;
    String deploymentUnit;
    long version;
    long updatedAtMs;

    public static RouteView from(TenantRoute route) {
        return RouteView.builder()
                .tenantId(route.getTenantId())
                .homeZone(route.getHomeZone())
                .zone(route.getZone())
                .cell(route.getCell())
                .deploymentUnit(route.getDeploymentUnit())
                .version(route.getVersion())
                .updatedAtMs(route.getUpdatedAt().toEpochMilli())
                .build();
    }
}
