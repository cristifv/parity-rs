package parity.rs;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Two instances share the requests topic and are stopped at the same moment: they drain one
 * at a time, each creates its drained file, and every request's offset is committed.
 */
@Testcontainers
class GracefulShutdownIntegrationTest {

    private static final String REQUESTS = "tenant.routing.requests";
    private static final String RESPONSES = "tenant.routing.responses";
    private static final String EVENTS = "tenant.routing.events";
    private static final String INSTANCES = "tenant.routing.instances";

    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    static EmbeddedKafkaKraftBroker broker;

    @TempDir
    static Path dir;

    private final JsonMapper json = JsonMapper.builder().build();

    @BeforeAll
    static void startBroker() {
        broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.afterPropertiesSet();
        broker.addTopics(new NewTopic(REQUESTS, 2, (short) 1), new NewTopic(RESPONSES, 1, (short) 1),
                new NewTopic(EVENTS, 1, (short) 1), new NewTopic(INSTANCES, 1, (short) 1));
    }

    @AfterAll
    static void stopBroker() {
        broker.destroy();
    }

    /** Settings as command-line arguments, which win over application.yml. */
    private ConfigurableApplicationContext start(String instance) {
        return new SpringApplicationBuilder(ParityRsApplication.class).run(
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--parity-rs.lifecycle.instance-id=" + instance,
                "--parity-rs.lifecycle.healthy-file=" + dir.resolve(instance + ".healthy"),
                "--parity-rs.lifecycle.drained-file=" + dir.resolve(instance + ".drained"),
                "--logging.level.kafka=off", "--logging.level.org.apache.kafka.storage=off");
    }

    @Test
    void instancesDrainOneAtATime() throws Exception {
        // Left over from an earlier run: removed at startup.
        Files.write(dir.resolve("a.healthy"), new byte[0]);
        Files.write(dir.resolve("a.drained"), new byte[0]);

        ConfigurableApplicationContext a = start("a");
        assertThat(dir.resolve("a.drained")).doesNotExist();
        assertThat(dir.resolve("a.healthy")).exists();
        ConfigurableApplicationContext b = start("b");
        assertThat(dir.resolve("b.healthy")).exists();

        Set<String> ids = sendRequests(40);
        awaitResponses(ids);

        CompletableFuture.allOf(CompletableFuture.runAsync(a::close), CompletableFuture.runAsync(b::close))
                .get();

        assertThat(dir.resolve("a.drained")).exists();
        assertThat(dir.resolve("b.drained")).exists();
        assertThat(dir.resolve("a.healthy")).doesNotExist();
        assertThat(dir.resolve("b.healthy")).doesNotExist();

        List<String> lifecycle = readAll(INSTANCES).stream()
                .map(e -> e.get("instance").asString() + " " + e.get("type").asString())
                .toList();
        String first = lifecycle.stream().filter(e -> e.endsWith(" instance.draining")).findFirst()
                .orElseThrow().split(" ")[0];
        String second = first.equals("a") ? "b" : "a";
        assertThat(lifecycle).contains(
                "a instance.started", "b instance.started",
                "a instance.drain_requested", "b instance.drain_requested");
        assertThat(lifecycle.indexOf(first + " instance.drained"))
                .as("%s drains completely before %s starts draining: %s", first, second, lifecycle)
                .isLessThan(lifecycle.indexOf(second + " instance.draining"));
        assertThat(lifecycle.indexOf(second + " instance.drained")).isGreaterThan(0);
        assertThat(lifecycle).contains("a instance.stopped", "b instance.stopped");

        assertEveryRequestCommitted();
    }

    private Set<String> sendRequests(int count) {
        Set<String> ids = new HashSet<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()),
                new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < count; i++) {
                String id = UUID.randomUUID().toString();
                String tenant = "t" + (i % 8);
                ids.add(id);
                producer.send(new ProducerRecord<>(REQUESTS, tenant, """
                        {"id": "%s", "cmd": "set_route", "tenant_id": "%s", "home_zone": "eu1",
                         "zone": "eu1", "cell": "eu1a", "deployment_unit": "unit-%d"}""".formatted(id, tenant, i)));
            }
        }
        return ids;
    }

    private void awaitResponses(Set<String> ids) {
        Instant deadline = Instant.now().plusSeconds(60);
        Set<String> seen = new HashSet<>();
        while (!seen.containsAll(ids) && Instant.now().isBefore(deadline)) {
            seen = readAll(RESPONSES).stream().map(r -> r.get("id").asString()).collect(Collectors.toSet());
        }
        assertThat(seen).containsAll(ids);
    }

    private List<JsonNode> readAll(String topic) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new StringDeserializer())) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            List<JsonNode> values = new ArrayList<>();
            while (consumer.position(partition) < end) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    values.add(json.readTree(record.value()));
                }
            }
            return values;
        }
    }

    private void assertEveryRequestCommitted() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets("parity-rs").partitionsToOffsetAndMetadata().get();
            for (int p = 0; p < 2; p++) {
                TopicPartition partition = new TopicPartition(REQUESTS, p);
                long end = admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all().get()
                        .get(partition).offset();
                OffsetAndMetadata offset = committed.get(partition);
                assertThat(offset == null ? 0 : offset.offset()).as("committed offset of %s", partition)
                        .isEqualTo(end);
            }
        }
    }
}
