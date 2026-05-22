package io.unitycatalog.client.delta;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.ErrorModel;
import io.unitycatalog.client.delta.model.ErrorResponse;
import io.unitycatalog.client.delta.model.ErrorType;
import java.util.Optional;

/**
 * Delta-specific specialization of {@link ApiException}. Parses the spec-defined error envelope
 * ({@code {"error": {"code", "type", "message", "stack"}}}) once at construction and exposes typed
 * accessors so call sites don't repeat the parse-from-responseBody dance.
 *
 * <p>Designed to wrap an existing {@link ApiException}: callers that catch the generated base type
 * can upgrade it with {@link #from(ApiException)} when they want the typed view. The wrapping is
 * caller-driven for now; the structure is set up so a future {@code ApiClient} interceptor can do
 * the wrap automatically without touching call sites that already use this class. Until then, both
 * shapes co-exist:
 *
 * <pre>{@code
 * try {
 *   deltaTablesApi.updateTable(...);
 * } catch (ApiException ex) {
 *   DeltaApiException.from(ex).ifPresent(d -> {
 *     ErrorType type = d.getErrorType();      // spec-defined enum
 *     String message = d.getErrorMessage();   // spec-defined message
 *     // ...
 *   });
 *   throw ex;
 * }
 * }</pre>
 */
public class DeltaApiException extends ApiException {

  private static final ObjectMapper MAPPER = createObjectMapper();

  protected static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
    return mapper;
  }

  /** {@code null} if the response body wasn't parseable as a Delta error envelope. */
  private final ErrorModel error;

  /**
   * Wrap an existing {@link ApiException}, copying its HTTP code, headers, body, cause, and message
   * through, and parsing the response body into the Delta error envelope. If the body isn't
   * parseable, {@link #getError()} (and the typed accessors derived from it) return null; callers
   * wanting a hard parse failure should use {@link #from(ApiException)} instead and react to an
   * empty {@link Optional}.
   */
  public DeltaApiException(ApiException source) {
    super(
        source.getMessage(),
        source.getCause(),
        source.getCode(),
        source.getResponseHeaders(),
        source.getResponseBody());
    this.error = parse(source.getResponseBody());
  }

  /**
   * Lenient upgrade: returns a {@link DeltaApiException} only when the response body parses cleanly
   * as a Delta error envelope. {@link Optional#empty()} typically means the response wasn't from
   * the Delta surface, or the server emitted a non-spec body -- in either case the caller can fall
   * back to the generic {@link ApiException} it was given.
   */
  public static Optional<DeltaApiException> from(ApiException ex) {
    DeltaApiException upgraded = new DeltaApiException(ex);
    return upgraded.error == null ? Optional.empty() : Optional.of(upgraded);
  }

  /** The full parsed error envelope. {@code null} if the body wasn't a Delta error response. */
  public ErrorModel getError() {
    return error;
  }

  /** {@code error.code}: spec-defined HTTP status code (typically matches {@link #getCode()}). */
  public Integer getErrorCode() {
    return error == null ? null : error.getCode();
  }

  /** {@code error.type}: spec-defined error-type enum, e.g. {@code TABLE_NOT_FOUND_EXCEPTION}. */
  public ErrorType getErrorType() {
    return error == null ? null : error.getType();
  }

  /** {@code error.message}: spec-defined human-readable message. */
  public String getErrorMessage() {
    return error == null ? null : error.getMessage();
  }

  private static ErrorModel parse(String body) {
    if (body == null || body.isEmpty()) {
      return null;
    }
    try {
      ErrorResponse response = MAPPER.readValue(body, ErrorResponse.class);
      return response == null ? null : response.getError();
    } catch (Exception e) {
      return null;
    }
  }
}
