package parity.rs.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Startup and graceful shutdown ({@code parity-rs.lifecycle.*}). The two files are empty
 * markers for the orchestrator (Kubernetes or Docker Swarm): the healthy file exists while
 * the service is up and taking work, the drained file once it has drained and can be
 * stopped. {@code instance-id} names this instance in the instances' lifecycle events;
 * empty means host name and process id.
 */
@Validated
@ConfigurationProperties("parity-rs.lifecycle")
public record LifecycleProperties(
        String instanceId,
        @NotBlank String healthyFile,
        @NotBlank String drainedFile,
        @NotNull Duration drainTimeout) {
}
