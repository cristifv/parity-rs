package parity.rs.api;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import parity.rs.config.TopicsProperties;
import parity.rs.queue.TenantQueues;

/**
 * Runs a queued request on its worker: handles it, writes the response, then acknowledges
 * it so its offset can be committed. A Postgres or Kafka failure is thrown, and the worker
 * retries the request.
 */
@Component
@RequiredArgsConstructor
public class RequestProcessor implements TenantQueues.JobProcessor {

    private final RequestHandler handler;
    private final KafkaTemplate<String, String> kafka;
    private final TopicsProperties topics;

    @Override
    public void process(TenantQueues.Job job) {
        RequestHandler.Reply reply = handler.handle(job.payload());
        String topic = reply.getReplyTo() != null ? reply.getReplyTo() : topics.responses();
        kafka.send(topic, reply.getKey(), reply.getBody()).join();
        job.ack().acknowledge();
    }
}
