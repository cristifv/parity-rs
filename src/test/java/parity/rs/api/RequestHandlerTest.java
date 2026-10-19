package parity.rs.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.validation.Validation;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import parity.rs.route.Placement;
import parity.rs.route.RouteView;
import parity.rs.route.SetRouteResult;
import parity.rs.route.TenantRouteService;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class RequestHandlerTest {

    private static final RouteView ACME = RouteView.builder()
            .tenantId("acme").homeZone("eu1").zone("eu1").cell("eu1a").deploymentUnit("bin-a")
            .version(0).updatedAtMs(1000).build();
    private static final String ACME_JSON = "{\"tenant_id\":\"acme\",\"home_zone\":\"eu1\",\"zone\":\"eu1\","
            + "\"cell\":\"eu1a\",\"deployment_unit\":\"bin-a\",\"version\":0,\"updated_at_ms\":1000}";

    @Mock
    private TenantRouteService routes;

    private RequestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new RequestHandler(
                routes, JsonMapper.builder().build(),
                Validation.buildDefaultValidatorFactory().getValidator());
    }

    @Test
    void ping() {
        RequestHandler.Reply reply = handler.handle("{\"id\":\"1\",\"cmd\":\"ping\"}");
        assertThat(reply.getBody()).isEqualTo("{\"id\":\"1\",\"ok\":true,\"result\":{\"pong\":true}}");
        assertThat(reply.getKey()).isEqualTo("1");
        assertThat(reply.getReplyTo()).isNull();
    }

    @Test
    void setRoute() {
        Placement placement = new Placement("eu1", "eu1", "eu1a", "bin-a");
        when(routes.setRoute("acme", placement)).thenReturn(new SetRouteResult(ACME, true));

        RequestHandler.Reply reply = handler.handle("""
                {"id": "r1", "cmd": "set_route", "tenant_id": "acme", "home_zone": "eu1",
                 "zone": "eu1", "cell": "eu1a", "deployment_unit": "bin-a"}""");

        assertThat(reply.getBody()).isEqualTo(
                "{\"id\":\"r1\",\"ok\":true,\"result\":{\"route\":" + ACME_JSON + ",\"changed\":true}}");
        verify(routes).setRoute("acme", placement);
    }

    @Test
    void setRouteListsEveryMissingField() {
        RequestHandler.Reply reply = handler.handle("{\"cmd\":\"set_route\",\"zone\":\"eu1\"}");
        assertThat(reply.getBody()).isEqualTo("{\"id\":null,\"ok\":false,\"error\":{\"code\":\"bad_request\","
                + "\"message\":\"cell is required; deployment_unit is required; home_zone is required; "
                + "tenant_id is required\"}}");
        verifyNoInteractions(routes);
    }

    @Test
    void getRoute() {
        when(routes.getRoute("acme")).thenReturn(Optional.of(ACME));
        assertThat(handler.handle("{\"id\":2,\"cmd\":\"get_route\",\"tenant_id\":\"acme\"}").getBody())
                .isEqualTo("{\"id\":2,\"ok\":true,\"result\":{\"route\":" + ACME_JSON + "}}");
    }

    @Test
    void unknownTenantIsNotFound() {
        when(routes.getRoute("globex")).thenReturn(Optional.empty());
        assertThat(handler.handle("{\"cmd\":\"get_route\",\"tenant_id\":\"globex\"}").getBody())
                .isEqualTo("{\"id\":null,\"ok\":false,\"error\":{\"code\":\"not_found\","
                        + "\"message\":\"no route for tenant globex\"}}");
    }

    @Test
    void deleteRoute() {
        when(routes.deleteRoute("acme")).thenReturn(Optional.of(ACME));
        assertThat(handler.handle("{\"cmd\":\"delete_route\",\"tenant_id\":\"acme\"}").getBody())
                .isEqualTo("{\"id\":null,\"ok\":true,\"result\":{\"route\":" + ACME_JSON + "}}");
    }

    @Test
    void listRoutes() {
        when(routes.listRoutes()).thenReturn(List.of(ACME));
        assertThat(handler.handle("{\"cmd\":\"list_routes\"}").getBody())
                .isEqualTo("{\"id\":null,\"ok\":true,\"result\":{\"routes\":[" + ACME_JSON + "]}}");
    }

    @Test
    void numericIdIsEchoedAndUsedAsKey() {
        RequestHandler.Reply reply = handler.handle("{\"id\":7,\"cmd\":\"ping\"}");
        assertThat(reply.getBody()).startsWith("{\"id\":7,");
        assertThat(reply.getKey()).isEqualTo("7");
    }

    @Test
    void replyTo() {
        assertThat(handler.handle("{\"cmd\":\"ping\",\"reply_to\":\"my.replies\"}").getReplyTo())
                .isEqualTo("my.replies");
    }

    @Test
    void invalidRequests() {
        assertThat(handler.handle("{not json").getBody()).contains("\"code\":\"bad_request\"", "invalid JSON");
        assertThat(handler.handle("[1,2]").getBody()).contains("request must be a JSON object");
        assertThat(handler.handle("").getBody()).contains("empty request");
        assertThat(handler.handle("{\"id\":\"x\"}").getBody())
                .isEqualTo("{\"id\":\"x\",\"ok\":false,\"error\":{\"code\":\"bad_request\",\"message\":\"cmd is required\"}}");
        assertThat(handler.handle("{\"cmd\":\"fly\"}").getBody()).contains("unknown cmd: fly");
        assertThat(handler.handle("{\"cmd\":\"get_route\",\"tenant_id\":{}}").getBody())
                .contains("\"code\":\"bad_request\"", "invalid request");
    }

    @Test
    void databaseFailuresAreThrownForRetry() {
        when(routes.getRoute("acme")).thenThrow(new DataAccessResourceFailureException("db down"));
        assertThatThrownBy(() -> handler.handle("{\"cmd\":\"get_route\",\"tenant_id\":\"acme\"}"))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }

    @Test
    void otherFailuresAreInternalErrors() {
        when(routes.listRoutes()).thenThrow(new IllegalStateException("bug"));
        assertThat(handler.handle("{\"id\":\"9\",\"cmd\":\"list_routes\"}").getBody())
                .isEqualTo("{\"id\":\"9\",\"ok\":false,\"error\":{\"code\":\"internal\","
                        + "\"message\":\"java.lang.IllegalStateException: bug\"}}");
    }
}
