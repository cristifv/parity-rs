package parity.rs.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Value;
import tools.jackson.databind.JsonNode;

/**
 * A response: {@code {"id", "ok": true, "result"}} or
 * {@code {"id", "ok": false, "error": {"code", "message"}}}.
 */
@Value
@JsonPropertyOrder({"id", "ok", "result", "error"})
public class Response {

    @JsonInclude(JsonInclude.Include.ALWAYS)
    JsonNode id;

    boolean ok;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Object result;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    ErrorBody error;

    @Value
    @JsonPropertyOrder({"code", "message"})
    public static class ErrorBody {
        String code;
        String message;
    }

    static Response ok(JsonNode id, Object result) {
        return new Response(id, true, result, null);
    }

    static Response error(JsonNode id, String code, String message) {
        return new Response(id, false, null, new ErrorBody(code, message));
    }
}
