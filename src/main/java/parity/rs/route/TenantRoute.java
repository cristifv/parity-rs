package parity.rs.route;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * A tenant's route: the zone, cell and deployment unit that serve it, and its home zone.
 * {@code version} is 0 for a new route and goes up by one on every change.
 */
@Entity
@Table(name = "tenant_route")
@Getter
@ToString
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TenantRoute {

    @Id
    private String tenantId;

    @Column(nullable = false)
    private String homeZone;

    @Column(nullable = false)
    private String zone;

    @Column(nullable = false)
    private String cell;

    @Column(nullable = false)
    private String deploymentUnit;

    @Version
    private Long version;

    @UpdateTimestamp
    private Instant updatedAt;

    TenantRoute(String tenantId) {
        this.tenantId = tenantId;
    }

    boolean hasPlacement(Placement placement) {
        return placement.equals(placement());
    }

    Placement placement() {
        return new Placement(homeZone, zone, cell, deploymentUnit);
    }

    void place(Placement placement) {
        homeZone = placement.getHomeZone();
        zone = placement.getZone();
        cell = placement.getCell();
        deploymentUnit = placement.getDeploymentUnit();
    }
}
