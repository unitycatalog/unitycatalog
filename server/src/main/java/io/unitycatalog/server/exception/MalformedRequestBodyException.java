package io.unitycatalog.server.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Objects;

/**
 * Thrown by {@link TypedErrorJacksonRequestConverter} when Jackson body deserialization fails. The
 * message is built here from {@link JsonProcessingException#getOriginalMessage()}, which drops
 * Jackson's source-location footer, so {@link BaseExceptionHandler} maps it to {@code 400
 * INVALID_ARGUMENT} through its existing {@link IllegalArgumentException} branch with no special
 * case. The Jackson failure stays the cause, so converters that inspect it (such as the Iceberg
 * surface's malformed-body rewrite) see the same shape as Armeria's own wrapper.
 */
final class MalformedRequestBodyException extends IllegalArgumentException {
  MalformedRequestBodyException(JsonProcessingException cause) {
    super(
        "Malformed request body: "
            + Objects.requireNonNullElse(cause.getOriginalMessage(), "invalid JSON"),
        cause);
  }
}
