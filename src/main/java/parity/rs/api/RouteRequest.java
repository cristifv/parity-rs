package parity.rs.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.NoArgsConstructor;
import parity.rs.route.Placement;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

/**
 * A request from the requests topic. Which fields are required depends on {@code cmd}:
 * the validation groups below.
 */
@Data
@NoArgsConstructor
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RouteRequest {

    /** Commands that name a tenant. */
    public interface TenantCommand {}

    /** {@code set_route}. */
    public interface SetRoute extends TenantCommand {}

    /** Echoed back in the response, any JSON value. */
    private JsonNode id;

    @NotBlank(message = "cmd is required")
    private String cmd;

    /** Topic for the response instead of the default responses topic. */
    private String replyTo;

    @NotBlank(groups = TenantCommand.class, message = "tenant_id is required")
    private String tenantId;

    @NotBlank(groups = SetRoute.class, message = "home_zone is required")
    private String homeZone;

    @NotBlank(groups = SetRoute.class, message = "zone is required")
    private String zone;

    @NotBlank(groups = SetRoute.class, message = "cell is required")
    private String cell;

    @NotBlank(groups = SetRoute.class, message = "deployment_unit is required")
    private String deploymentUnit;

    public Placement placement() {
        return new Placement(homeZone, zone, cell, deploymentUnit);
    }
}
