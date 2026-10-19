package parity.rs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** parity-rs: the tenant routing service. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ParityRsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ParityRsApplication.class, args);
    }
}
