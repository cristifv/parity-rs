package parity.rs;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** End to end: requests over an embedded Kafka broker, routes in a real Postgres. */
// The embedded broker logs storage errors while the JVM deletes its log dir at exit.
@SpringBootTest(properties = {
        "logging.level.kafka=off", "logging.level.org.apache.kafka.storage=off",
        "parity-rs.lifecycle.instance-id=it",
        "parity-rs.lifecycle.healthy-file=${java.io.tmpdir}/parity-rs-it.healthy",
        "parity-rs.lifecycle.drained-file=${java.io.tmpdir}/parity-rs-it.drained"})
@Testcontainers
@EmbeddedKafka(
        partitions = 1,
        topics = {ParityRsIntegrationTest.REQUESTS, ParityRsIntegrationTest.RESPONSES,
                  ParityRsIntegrationTest.EVENTS, ParityRsIntegrationTest.REPLIES,
                  ParityRsIntegrationTest.INSTANCES},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ParityRsIntegrationTest {

    static final String REQUESTS = "tenant.routing.requests";
    static final String RESPONSES = "tenant.routing.responses";
    static final String EVENTS = "tenant.routing.events";
    static final String REPLIES = "test.replies";
    static final String INSTANCES = "tenant.routing.instances";

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JsonMapper json;

    private Consumer<String, String> consumer;
    private final Map<String, Map<String, JsonNode>> byTopicAndKey = new HashMap<>();
    private final List<JsonNode> events = new ArrayList<>();

    @BeforeEach
    void subscribe() {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new StringDeserializer());
        List<TopicPartition> partitions = Stream.of(RESPONSES, EVENTS, REPLIES)
                .map(topic -> new TopicPartition(topic, 0))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    @Test
    void routesATenantAndPublishesEveryChange() {
        String tenant = "acme-" + UUID.randomUUID();

        JsonNode created = request("""
                {"id": "1", "cmd": "set_route", "tenant_id": "%s", "home_zone": "eu1",
                 "zone": "eu1", "cell": "eu1a", "deployment_unit": "bin-a"}""".formatted(tenant));
        assertThat(created.get("ok").asBoolean()).isTrue();
        assertThat(created.at("/result/changed").asBoolean()).isTrue();
        assertThat(created.at("/result/route/version").asLong()).isZero();
        assertThat(created.at("/result/route/deployment_unit").asString()).isEqualTo("bin-a");

        JsonNode unchanged = request("""
                {"id": "2", "cmd": "set_route", "tenant_id": "%s", "home_zone": "eu1",
                 "zone": "eu1", "cell": "eu1a", "deployment_unit": "bin-a"}""".formatted(tenant));
        assertThat(unchanged.at("/result/changed").asBoolean()).isFalse();
        assertThat(unchanged.at("/result/route/version").asLong()).isZero();

        JsonNode moved = request("""
                {"id": "3", "cmd": "set_route", "tenant_id": "%s", "home_zone": "eu1",
                 "zone": "eu1", "cell": "eu1b", "deployment_unit": "bin-b"}""".formatted(tenant));
        assertThat(moved.at("/result/changed").asBoolean()).isTrue();
        assertThat(moved.at("/result/route/version").asLong()).isEqualTo(1);

        JsonNode got = request("{\"id\": \"4\", \"cmd\": \"get_route\", \"tenant_id\": \"%s\"}".formatted(tenant));
        assertThat(got.at("/result/route/cell").asString()).isEqualTo("eu1b");

        JsonNode listed = request("{\"id\": \"5\", \"cmd\": \"list_routes\"}");
        assertThat(listed.at("/result/routes").valueStream().map(r -> r.get("tenant_id").asString()))
                .contains(tenant);

        JsonNode deleted = request("{\"id\": \"6\", \"cmd\": \"delete_route\", \"tenant_id\": \"%s\"}".formatted(tenant));
        assertThat(deleted.at("/result/route/version").asLong()).isEqualTo(1);

        JsonNode gone = request("{\"id\": \"7\", \"cmd\": \"get_route\", \"tenant_id\": \"%s\"}".formatted(tenant));
        assertThat(gone.at("/error/code").asString()).isEqualTo("not_found");

        // Each event is written before its response, so all three are there by now. The
        // unchanged set_route published nothing.
        List<JsonNode> tenantEvents = awaitEvents(tenant, 3);
        assertThat(tenantEvents).extracting(e -> e.get("type").asString())
                .containsExactly("route.changed", "route.changed", "route.deleted");
        assertThat(tenantEvents.get(0).has("previous")).isFalse();
        assertThat(tenantEvents.get(1).at("/previous/deployment_unit").asString()).isEqualTo("bin-a");
        assertThat(tenantEvents.get(1).at("/route/deployment_unit").asString()).isEqualTo("bin-b");
        assertThat(tenantEvents.get(2).at("/route/cell").asString()).isEqualTo("eu1b");
    }

    @Test
    void tenantsRunInParallelAndEachTenantStaysInOrder() {
        List<String> tenants = Stream.of("a", "b", "c", "d", "e")
                .map(t -> t + "-" + UUID.randomUUID()).toList();
        List<String> ids = new ArrayList<>();
        for (int unit = 0; unit < 10; unit++) {
            for (String tenant : tenants) {
                ids.add(send("""
                        {"id": "o", "cmd": "set_route", "tenant_id": "%s", "home_zone": "eu1",
                         "zone": "eu1", "cell": "eu1a", "deployment_unit": "unit-%d"}""".formatted(tenant, unit)));
            }
        }
        ids.forEach(id -> assertThat(await(RESPONSES, id).get("ok").asBoolean()).isTrue());

        for (String tenant : tenants) {
            List<JsonNode> tenantEvents = awaitEvents(tenant, 10);
            assertThat(tenantEvents).extracting(e -> e.at("/route/version").asLong())
                    .containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
            assertThat(tenantEvents).extracting(e -> e.at("/route/deployment_unit").asString())
                    .containsExactly("unit-0", "unit-1", "unit-2", "unit-3", "unit-4",
                            "unit-5", "unit-6", "unit-7", "unit-8", "unit-9");
        }
    }

    @Test
    void replyToAndBadRequests() {
        String id = UUID.randomUUID().toString();
        kafka.send(REQUESTS, "{\"id\": \"%s\", \"cmd\": \"ping\", \"reply_to\": \"%s\"}".formatted(id, REPLIES));
        assertThat(await(REPLIES, id).at("/result/pong").asBoolean()).isTrue();

        JsonNode bad = request("{\"id\": \"b1\", \"cmd\": \"set_route\", \"tenant_id\": \"acme\"}");
        assertThat(bad.at("/error/code").asString()).isEqualTo("bad_request");
        assertThat(bad.at("/error/message").asString())
                .isEqualTo("cell is required; deployment_unit is required; home_zone is required; zone is required");
    }

    /** Sends a request (ids are made unique per call) and waits for its response. */
    private JsonNode request(String body) {
        return await(RESPONSES, send(body));
    }

    /** Sends a request without waiting, keyed by its tenant. Returns its unique id. */
    private String send(String body) {
        JsonNode tree = json.readTree(body);
        String id = tree.get("id").asString() + "-" + UUID.randomUUID();
        ((tools.jackson.databind.node.ObjectNode) tree).put("id", id);
        JsonNode tenant = tree.get("tenant_id");
        kafka.send(REQUESTS, tenant == null ? null : tenant.asString(), json.writeValueAsString(tree));
        return id;
    }

    private JsonNode await(String topic, String key) {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            JsonNode found = byTopicAndKey.getOrDefault(topic, Map.of()).get(key);
            if (found != null) {
                return found;
            }
            poll();
        }
        throw new AssertionError("no message with key " + key + " on " + topic);
    }

    private List<JsonNode> awaitEvents(String tenant, int count) {
        Instant deadline = Instant.now().plusSeconds(30);
        List<JsonNode> found = List.of();
        while (Instant.now().isBefore(deadline)) {
            found = events.stream().filter(e -> e.get("tenant_id").asString().equals(tenant)).toList();
            if (found.size() >= count) {
                return found;
            }
            poll();
        }
        throw new AssertionError("expected " + count + " events for " + tenant + ", got " + found);
    }

    private void poll() {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
            JsonNode value = json.readTree(record.value());
            if (record.topic().equals(EVENTS)) {
                events.add(value);
            } else {
                byTopicAndKey.computeIfAbsent(record.topic(), t -> new HashMap<>()).put(record.key(), value);
            }
        }
    }
}
