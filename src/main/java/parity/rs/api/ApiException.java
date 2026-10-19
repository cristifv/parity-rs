package parity.rs.api;

import lombok.Getter;

/** A request that failed, with the error code sent back in the response. */
@Getter
public class ApiException extends RuntimeException {

    private final String code;

    private ApiException(String code, String message) {
        super(message);
        this.code = code;
    }

    public static ApiException badRequest(String message) {
        return new ApiException("bad_request", message);
    }

    public static ApiException notFound(String message) {
        return new ApiException("not_found", message);
    }
}
