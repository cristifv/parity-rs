package parity.rs.api;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import parity.rs.queue.TenantQueues;

/**
 * The public API: takes requests from Kafka and queues each one on its tenant's queue.
 * Offsets are committed as requests are acknowledged, and an offset is only committed
 * once every earlier request of its partition is done (ack mode {@code manual} with async
 * acks).
 */
@Component
@RequiredArgsConstructor
public class RequestListener {

    public static final String ID = "requests";

    private final RequestHandler handler;
    private final TenantQueues queues;

    @KafkaListener(id = ID, idIsGroup = false, topics = "${parity-rs.topics.requests}")
    public void onRequest(ConsumerRecord<String, String> request, Acknowledgment ack) {
        queues.submit(handler.tenantOf(request.value()), new TenantQueues.Job(request.value(), ack));
    }
}
