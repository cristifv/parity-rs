package parity.rs.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** The tenant queues ({@code parity-rs.workers.*}): the threads that run them, and retries. */
@Validated
@ConfigurationProperties("parity-rs.workers")
public record WorkersProperties(
        @Min(1) int count,
        @NotNull Duration retryDelay) {
}
