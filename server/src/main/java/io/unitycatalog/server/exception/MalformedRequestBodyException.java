package io.unitycatalog.server.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Objects;

/**
 * Thrown by {@link TypedErrorJacksonRequestConverter} when Jackson body deserialization fails. A
 * dedicated type (rather than reusing {@link IllegalArgumentException}, which Armeria's own Jackson
 * wrapper uses and which application code also throws for validation) gives {@link
 * BaseExceptionHandler} a type-precise signal for "malformed request body" without any shape
 * heuristics or cause-chain walking.
 *
 * <p>The cause is always a {@link JsonProcessingException} from Jackson; {@code
 * BaseExceptionHandler} surfaces its {@code getOriginalMessage()} on the wire.
 */
public final class MalformedRequestBodyException extends RuntimeException {
  private final JsonProcessingException jpe;

  public MalformedRequestBodyException(JsonProcessingException cause) {
    super(Objects.requireNonNull(cause, "cause"));
    this.jpe = cause;
  }

  public JsonProcessingException getJsonProcessingException() {
    return jpe;
  }
}
