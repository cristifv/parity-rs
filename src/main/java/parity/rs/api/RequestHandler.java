package parity.rs.api;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.groups.Default;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.KafkaException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import parity.rs.route.RouteView;
import parity.rs.route.TenantRouteService;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Runs one JSON request and builds its response. Client errors become error responses.
 * Database and Kafka failures are thrown, so the listener retries the request.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RequestHandler {

    /** The response to send: its topic ({@code reply_to}, or null for the default), key and JSON. */
    @Value
    public static class Reply {
        String replyTo;
        String key;
        String body;
    }

    @Value
    public static class RouteResult {
        RouteView route;
    }

    @Value
    public static class RouteListResult {
        List<RouteView> routes;
    }

    private final TenantRouteService routes;
    private final JsonMapper json;
    private final Validator validator;

    public Reply handle(String payload) {
        JsonNode id = null;
        String replyTo = null;
        Response response;
        try {
            JsonNode tree = parse(payload);
            id = tree.get("id");
            replyTo = text(tree.get("reply_to"));
            response = Response.ok(id, dispatch(convert(tree)));
        } catch (ApiException e) {
            response = Response.error(id, e.getCode(), e.getMessage());
        } catch (DataAccessException | TransactionException | KafkaException
                 | org.apache.kafka.common.KafkaException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("request failed: {}", payload, e);
            response = Response.error(id, "internal", e.toString());
        }
        return new Reply(replyTo, key(id), json.writeValueAsString(response));
    }

    /** The request's {@code tenant_id}, or null if it has none or isn't valid JSON. Picks its queue. */
    public String tenantOf(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            JsonNode tree = json.readTree(payload);
            return tree == null ? null : text(tree.get("tenant_id"));
        } catch (JacksonException e) {
            return null;
        }
    }

    private Object dispatch(RouteRequest request) {
        validate(request, Default.class);
        return switch (request.getCmd()) {
            case "ping" -> Map.of("pong", true);
            case "set_route" -> {
                validate(request, RouteRequest.SetRoute.class);
                yield routes.setRoute(request.getTenantId(), request.placement());
            }
            case "get_route" -> {
                validate(request, RouteRequest.TenantCommand.class);
                yield new RouteResult(routes.getRoute(request.getTenantId())
                        .orElseThrow(() -> notFound(request.getTenantId())));
            }
            case "delete_route" -> {
                validate(request, RouteRequest.TenantCommand.class);
                yield new RouteResult(routes.deleteRoute(request.getTenantId())
                        .orElseThrow(() -> notFound(request.getTenantId())));
            }
            case "list_routes" -> new RouteListResult(routes.listRoutes());
            default -> throw ApiException.badRequest("unknown cmd: " + request.getCmd());
        };
    }

    private void validate(RouteRequest request, Class<?> group) {
        Set<ConstraintViolation<RouteRequest>> violations = validator.validate(request, group);
        if (!violations.isEmpty()) {
            throw ApiException.badRequest(violations.stream()
                    .map(ConstraintViolation::getMessage)
                    .sorted()
                    .collect(Collectors.joining("; ")));
        }
    }

    private JsonNode parse(String payload) {
        if (payload == null || payload.isBlank()) {
            throw ApiException.badRequest("empty request");
        }
        JsonNode tree;
        try {
            tree = json.readTree(payload);
        } catch (JacksonException e) {
            throw ApiException.badRequest("invalid JSON: " + e.getOriginalMessage());
        }
        if (tree == null || !tree.isObject()) {
            throw ApiException.badRequest("request must be a JSON object");
        }
        return tree;
    }

    private RouteRequest convert(JsonNode tree) {
        try {
            return json.treeToValue(tree, RouteRequest.class);
        } catch (JacksonException e) {
            throw ApiException.badRequest("invalid request: " + e.getOriginalMessage());
        }
    }

    private static ApiException notFound(String tenantId) {
        return ApiException.notFound("no route for tenant " + tenantId);
    }

    private static String text(JsonNode node) {
        return node != null && node.isString() && !node.asString().isBlank() ? node.asString() : null;
    }

    private static String key(JsonNode id) {
        if (id == null || id.isNull()) {
            return null;
        }
        return id.isValueNode() ? id.asString() : id.toString();
    }
}
