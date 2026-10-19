package parity.rs.route;

import java.time.Clock;
import java.util.concurrent.CompletionException;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import parity.rs.config.TopicsProperties;
import tools.jackson.databind.json.JsonMapper;

/** Publishes route events and waits until Kafka has them. */
@Component
@RequiredArgsConstructor
public class RouteEventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final TopicsProperties topics;
    private final Clock clock;

    void routeChanged(RouteView route, RouteView previous) {
        publish(RouteEvent.builder()
                .type(RouteEvent.ROUTE_CHANGED)
                .atMs(clock.millis())
                .tenantId(route.getTenantId())
                .route(route)
                .previous(previous)
                .build());
    }

    void routeDeleted(RouteView route) {
        publish(RouteEvent.builder()
                .type(RouteEvent.ROUTE_DELETED)
                .atMs(clock.millis())
                .tenantId(route.getTenantId())
                .route(route)
                .build());
    }

    private void publish(RouteEvent event) {
        try {
            kafka.send(topics.events(), event.getTenantId(), json.writeValueAsString(event)).join();
        } catch (CompletionException e) {
            throw e.getCause() instanceof RuntimeException cause ? cause : e;
        }
    }
}
