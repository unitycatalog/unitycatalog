package io.unitycatalog.server.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.util.Objects;

/**
 * Thrown by {@link TypedErrorJacksonRequestConverter} when Jackson body deserialization fails. The
 * message is built here from {@link JsonProcessingException#getOriginalMessage()}, which drops
 * Jackson's source-location footer and reference chain, plus the JSON path of the failing field
 * (e.g. {@code (at columns[0].position)}) so the caller still learns which field was wrong. {@link
 * BaseExceptionHandler} maps it to {@code 400 INVALID_ARGUMENT} through its existing {@link
 * IllegalArgumentException} branch with no special case. The Jackson failure stays the cause, so
 * converters that inspect it (such as the Iceberg surface's malformed-body rewrite) see the same
 * shape as Armeria's own wrapper.
 */
final class MalformedRequestBodyException extends IllegalArgumentException {
  MalformedRequestBodyException(JsonProcessingException cause) {
    super("Malformed request body: " + describe(cause), cause);
  }

  private static String describe(JsonProcessingException cause) {
    String message = Objects.requireNonNullElse(cause.getOriginalMessage(), "invalid JSON");
    if (!(cause instanceof JsonMappingException mapping)) {
      return message;
    }
    StringBuilder path = new StringBuilder();
    for (JsonMappingException.Reference ref : mapping.getPath()) {
      if (ref.getFieldName() != null) {
        path.append(path.isEmpty() ? "" : ".").append(ref.getFieldName());
      } else if (ref.getIndex() >= 0) {
        path.append('[').append(ref.getIndex()).append(']');
      }
    }
    return path.isEmpty() ? message : message + " (at " + path + ")";
  }
}
