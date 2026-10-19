# parity-rs: tenant routing

parity-rs routes each tenant to the **zone**, **cell** and **deployment unit** that serve it,
and keeps the tenant's **home zone**. It does tenant routing only. Call routing (queues and
agents) is the routing engine built from Parity (`parity-poc`).

Its place in the platform is described in
`../contact-center-architecture/ARCHITECTURE.md` §3.22. Requirements, decisions and open
questions are in `PROGRESS.md`.

| | |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 4.1 (Spring Kafka, Spring Data JPA, Flyway, Bean Validation) |
| Build | Maven |
| Boilerplate | Lombok |
| Storage | PostgreSQL, table `tenant_route` |
| Public API | Kafka (Redpanda) |

## Code

| Package | Classes | Does |
|---|---|---|
| `parity.rs` | `ParityRsApplication` | Spring Boot entry point |
| `parity.rs.config` | `TopicsProperties`, `WorkersProperties`, `LifecycleProperties`, `ParityRsConfig` | Settings (`parity-rs.topics.*`, `.workers.*`, `.lifecycle.*`) and the clock |
| `parity.rs.api` | `RequestListener` | `@KafkaListener` on the requests topic. Puts each request on its tenant's queue |
| | `RequestProcessor` | Runs a queued request on its worker: handles it, writes the response, acknowledges it |
| | `RequestHandler` | Parses and validates a request, calls the service, builds the response |
| | `RouteRequest`, `Response`, `ApiException` | The request (validation groups per command), the response, client errors |
| `parity.rs.queue` | `TenantQueues` | The concurrent queues: one per tenant, in a `ConcurrentHashMap` keyed by tenant id |
| `parity.rs.route` | `TenantRouteService` | `@Transactional` route changes. Each change publishes its event before the commit |
| | `TenantRoute`, `TenantRouteRepository` | JPA entity (`@Version` optimistic locking) and Spring Data repository |
| | `RouteEventPublisher`, `RouteEvent` | Events on the events topic |
| | `RouteView`, `Placement`, `SetRouteResult` | A route in JSON; zone + cell + deployment unit + home zone; the result of `set_route` |
| `parity.rs.lifecycle` | `GracefulShutdown` | The healthy and drained files, and the drain on shutdown |
| | `InstanceCoordinator`, `DrainLine`, `InstanceEvent` | One-at-a-time drains, coordinated between instances over Kafka |

The schema is `src/main/resources/db/migration/V1__tenant_route.sql` (Flyway). Hibernate
only validates it (`ddl-auto: validate`).

## Run

```sh
docker run -d --name parity-rs-postgres -p 5432:5432 \
  -e POSTGRES_DB=parity_rs -e POSTGRES_USER=parity_rs -e POSTGRES_PASSWORD=parity_rs \
  postgres:16-alpine
docker run -d --name parity-rs-redpanda -p 19092:19092 \
  docker.redpanda.com/redpandadata/redpanda:latest redpanda start --mode dev-container \
  --smp 1 --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
  --advertise-kafka-addr internal://localhost:9092,external://localhost:19092
docker exec parity-rs-redpanda rpk topic create \
  tenant.routing.requests tenant.routing.responses tenant.routing.events
docker exec parity-rs-redpanda rpk topic create tenant.routing.instances -p 1

mvn package
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:19092 java -jar target/parity-rs.jar
```

Settings are in `src/main/resources/application.yml`. Any of them can be overridden with
an environment variable or a `--name=value` argument (Spring Boot's relaxed binding):

| Setting | Environment variable | Default |
|---|---|---|
| `spring.datasource.url` | `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/parity_rs` |
| `spring.datasource.username` / `password` | `SPRING_DATASOURCE_USERNAME` / `PASSWORD` | `parity_rs` / `parity_rs` |
| `spring.kafka.bootstrap-servers` | `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `spring.kafka.consumer.group-id` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `parity-rs` |
| `parity-rs.topics.requests` | `PARITY_RS_TOPICS_REQUESTS` | `tenant.routing.requests` |
| `parity-rs.topics.responses` | `PARITY_RS_TOPICS_RESPONSES` | `tenant.routing.responses` |
| `parity-rs.topics.events` | `PARITY_RS_TOPICS_EVENTS` | `tenant.routing.events` |
| `parity-rs.topics.instances` (one partition) | `PARITY_RS_TOPICS_INSTANCES` | `tenant.routing.instances` |
| `parity-rs.workers.count` | `PARITY_RS_WORKERS_COUNT` | `4` |
| `parity-rs.workers.retry-delay` | `PARITY_RS_WORKERS_RETRY_DELAY` | `1s` |
| `parity-rs.lifecycle.instance-id` | `PARITY_RS_LIFECYCLE_INSTANCE_ID` | empty: host name and process id |
| `parity-rs.lifecycle.healthy-file` | `PARITY_RS_LIFECYCLE_HEALTHY_FILE` | `/tmp/parity-rs.healthy` |
| `parity-rs.lifecycle.drained-file` | `PARITY_RS_LIFECYCLE_DRAINED_FILE` | `/tmp/parity-rs.drained` |
| `parity-rs.lifecycle.drain-timeout` | `PARITY_RS_LIFECYCLE_DRAIN_TIMEOUT` | `30s` |

Topics must exist beforehand (or the broker must auto-create them). parity-rs doesn't
create them. Neither does it create the `reply_to` topics of requests.

## Protocol

Same conventions as parity-ms. A request is one JSON object with a `cmd` field. The
optional `id` (any JSON value) is echoed back. The response key is the `id`. A response is
`{"id", "ok": true, "result"}` or `{"id", "ok": false, "error": {"code", "message"}}`. It
goes to the responses topic, or to the request's `reply_to` topic.

| cmd | Fields | Result |
|---|---|---|
| `set_route` | `tenant_id`, `home_zone`, `zone`, `cell`, `deployment_unit` | `route`, and `changed` (false when the route was already that). Creates the route if needed |
| `get_route` | `tenant_id` | `route` |
| `delete_route` | `tenant_id` | `route`: the deleted route |
| `list_routes` | none | `routes`, ordered by tenant id |
| `ping` | none | `{"pong": true}` |

A route is `{"tenant_id", "home_zone", "zone", "cell", "deployment_unit", "version",
"updated_at_ms"}`. `version` is 0 for a new route and goes up by one on every change. A
deleted route that is set again starts at 0.

```json
{"id": "r1", "cmd": "set_route", "tenant_id": "acme", "home_zone": "eu1",
 "zone": "eu1", "cell": "eu1a", "deployment_unit": "bin-a"}

{"id": "r1", "ok": true, "result": {"route": {"tenant_id": "acme", "home_zone": "eu1",
 "zone": "eu1", "cell": "eu1a", "deployment_unit": "bin-a", "version": 0,
 "updated_at_ms": 1791417600000}, "changed": true}}
```

Error codes:
- `bad_request`: invalid JSON, a missing or empty field (all of them are listed), or an
  unknown `cmd`;
- `not_found`: no route for the tenant;
- `internal`: an unexpected failure.

## Events

On the events topic, keyed by tenant id. Each has `type`, `at_ms` and `tenant_id`.

| type | Fields |
|---|---|
| `route.changed` | `route`; `previous`, except for a new tenant |
| `route.deleted` | `route`: the deleted route |

An unchanged `set_route` publishes nothing.

## Concurrency: tenant queues

- One queue per tenant, in a `ConcurrentHashMap` keyed by `tenant_id`. The value is the
  tenant's last request, a `CompletableFuture`. A new request is chained after it
  (`compute`) and takes its place.
- So one tenant's requests run in order, one at a time, and different tenants run in
  parallel on a pool of `workers.count` threads (`worker-0` …).
- A tenant leaves the map once its last request has run.
- Requests without a tenant (`list_routes`, `ping`, invalid ones) share one queue.
- No queue limit is needed: with async acks, Kafka pauses the consumer until every request
  of a poll (at most `max.poll.records`, 500) is done.
- A request that fails on Postgres or Kafka is retried every `workers.retry-delay` until it
  works. That holds up only its tenant's queue.

## Delivery

- **At-least-once.** Each worker acknowledges a request after its response is written.
  Offsets are committed in order: a request's offset is committed only once every earlier
  request of its partition is done (ack mode `manual` with async acks). After a crash,
  Kafka delivers the unacknowledged requests again.
- **A change and its event go together.** The event is published, and Kafka has
  acknowledged it, before the Postgres transaction commits. If the publish fails, the change
  is rolled back.
- **Client errors aren't retried:** they become error responses.
- **A retried request runs again.** `set_route` is idempotent. A retried `delete_route`
  answers `not_found` if the first try deleted the route.
- **Concurrent changes** to one tenant from several instances are caught by optimistic
  locking on `version`, and the losing request is retried.

## Startup and graceful shutdown

The orchestrator (Kubernetes or Docker Swarm) is told about the service's state with two
empty files, named after the service:

| File | Created | Removed |
|---|---|---|
| `/tmp/parity-rs.healthy` | When the service has started (`ApplicationReadyEvent`) | When it starts draining |
| `/tmp/parity-rs.drained` | When it has drained and can be stopped | At the next start |

Both are removed at startup, so files left by an earlier run never count. A health check
tests for the healthy file, e.g. `test -f /tmp/parity-rs.healthy`.

**On SIGTERM** (or any normal shutdown of the Spring context):
1. **Wait for its turn.** Instances drain one at a time (below). Until its turn, the instance
   keeps serving.
2. **Stop taking requests:** remove the healthy file and pause the Kafka listener.
3. **Drain:** wait until every tenant queue is empty and every request is answered.
4. **Hand over:** stop the listener. This commits the offsets and leaves the consumer group,
   so the other instances take over its partitions.
5. **Signal:** create the drained file. The process then exits.

The wait and the drain share `lifecycle.drain-timeout` (30 s). If it runs out, there is no
drained file, and Kafka delivers the requests left to another instance. Give the
orchestrator a longer grace period: 45 s, as for the other Java services.

**Coordinated between instances over Kafka.** Each instance publishes its lifecycle events
to partition 0 of the instances topic, keyed by instance id:

| type | When |
|---|---|
| `instance.started` | Started (with the healthy file) |
| `instance.drain_requested` | SIGTERM: joins the drain line |
| `instance.draining` | Its turn: draining starts |
| `instance.drained` | Drained (with the drained file) |
| `instance.stopped` | Shutdown done, drained or not |

Each event has `type`, `service`, `instance`, `at_ms` and `drain_timeout_ms`.

- Every instance reads the topic back, from two drain timeouts ago. Since there's one
  partition, all instances see the same order.
- The drain line is the instances in the order of their `drain_requested` events. An
  instance drains when it is first in line.
- An instance leaves the line with its `drained`, `stopped` or `started` event, or once its
  request is older than its drain timeout plus 5 s: by then it has finished or was killed.
- So when several instances are stopped at once, they drain one after another, and the
  others keep serving meanwhile. The last instance drains without waiting.
- **Best effort:** if the instance can't publish its request, or doesn't read it back
  within 5 s, it drains without waiting.

## Tests

```sh
mvn test
```

- `RequestHandlerTest`: unit tests of requests, validation, responses and error handling,
  with the service mocked.
- `TenantQueuesTest`: one tenant's requests run in order, one at a time; other tenants run
  while one is blocked; a tenant leaves the map when done; retries keep the order; requests
  without a tenant share one queue.
- `DrainLineTest`: the order of the drain line, leaving it, stale entries.
- `ParityRsIntegrationTest`: end to end, with an embedded Kafka broker and Postgres 16 in a
  Testcontainers container (needs Docker): the route lifecycle, 5 tenants × 10 changes sent
  at once (each tenant's events in order), `reply_to`, bad requests.
- `GracefulShutdownIntegrationTest`: two instances on a two-partition requests topic,
  stopped at the same moment. They drain one after the other, both create their drained
  files, the healthy files are gone, stale files are removed at startup, and every
  request's offset is committed.
