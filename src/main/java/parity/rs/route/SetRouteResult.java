package parity.rs.route;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Value;

/** The tenant's route after a {@code set_route}, and whether it changed. */
@Value
@JsonPropertyOrder({"route", "changed"})
public class SetRouteResult {
    RouteView route;
    boolean changed;
}
