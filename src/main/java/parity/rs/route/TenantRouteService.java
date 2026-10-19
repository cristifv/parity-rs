package parity.rs.route;

import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tenant routes in Postgres. Each change publishes its event before the transaction
 * commits, so a failed publish rolls the change back.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TenantRouteService {

    private final TenantRouteRepository repository;
    private final RouteEventPublisher events;

    /** Routes the tenant to a zone, cell and deployment unit, and sets its home zone. */
    @Transactional
    public SetRouteResult setRoute(String tenantId, Placement placement) {
        Optional<TenantRoute> existing = repository.findById(tenantId);
        RouteView previous = existing.map(RouteView::from).orElse(null);
        if (existing.isPresent() && existing.get().hasPlacement(placement)) {
            return new SetRouteResult(previous, false);
        }

        TenantRoute route = existing.orElseGet(() -> new TenantRoute(tenantId));
        route.place(placement);
        RouteView current = RouteView.from(repository.saveAndFlush(route));
        events.routeChanged(current, previous);
        log.info("tenant {} routed to zone {}, cell {}, deployment unit {} (home zone {}), version {}",
                tenantId, current.getZone(), current.getCell(), current.getDeploymentUnit(),
                current.getHomeZone(), current.getVersion());
        return new SetRouteResult(current, true);
    }

    @Transactional(readOnly = true)
    public Optional<RouteView> getRoute(String tenantId) {
        return repository.findById(tenantId).map(RouteView::from);
    }

    /** All routes, ordered by tenant id. */
    @Transactional(readOnly = true)
    public List<RouteView> listRoutes() {
        return repository.findAll(Sort.by("tenantId")).stream().map(RouteView::from).toList();
    }

    /** Removes the tenant's route and returns it. */
    @Transactional
    public Optional<RouteView> deleteRoute(String tenantId) {
        Optional<TenantRoute> existing = repository.findById(tenantId);
        existing.ifPresent(route -> {
            repository.delete(route);
            repository.flush();
            events.routeDeleted(RouteView.from(route));
            log.info("tenant {} route deleted", tenantId);
        });
        return existing.map(RouteView::from);
    }
}
