package parity.rs.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The Kafka topics ({@code parity-rs.topics.*}): the public API, and the instances' lifecycle
 * events. The instances topic needs one partition, so every instance reads the same order.
 */
@Validated
@ConfigurationProperties("parity-rs.topics")
public record TopicsProperties(
        @NotBlank String requests,
        @NotBlank String responses,
        @NotBlank String events,
        @NotBlank String instances) {
}
