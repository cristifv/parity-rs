package parity.rs.route;

import lombok.Value;

/** Where a tenant goes: zone, cell and deployment unit, plus the tenant's home zone. */
@Value
public class Placement {
    String homeZone;
    String zone;
    String cell;
    String deploymentUnit;
}
