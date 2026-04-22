package io.unitycatalog.server.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import java.lang.reflect.ParameterizedType;
import java.util.Objects;

/**
 * {@link RequestConverterFunction} wrapper that replaces {@link JacksonRequestConverterFunction}'s
 * {@code IllegalArgumentException("failed to parse a JSON document: ...", jpe)} output with a
 * {@link MalformedRequestBodyException} whose message is {@code "Malformed request body: "} plus
 * Jackson's original message, without the exception-class prefix and source-location footer.
 * Because it is still an {@link IllegalArgumentException} with the Jackson failure as its cause,
 * {@link BaseExceptionHandler} maps it to {@code 400 INVALID_ARGUMENT} unchanged, and wrappers that
 * inspect the cause keep working.
 *
 * <p>{@code JacksonRequestConverterFunction} is {@code final}, so we delegate rather than subclass.
 * The catch is narrowed to the exact shape Armeria's Jackson wrapper produces: {@code
 * IllegalArgumentException} with a {@link JsonProcessingException} cause. Any other exception
 * (including Armeria's {@code FallthroughException}, a plain {@code RuntimeException}) propagates
 * unchanged.
 */
public final class TypedErrorJacksonRequestConverter implements RequestConverterFunction {

  private final JacksonRequestConverterFunction delegate;

  public TypedErrorJacksonRequestConverter(JacksonRequestConverterFunction delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  /** Convenience factory: builds a Jackson delegate from {@code mapper} and wraps it. */
  public static TypedErrorJacksonRequestConverter forMapper(ObjectMapper mapper) {
    return new TypedErrorJacksonRequestConverter(new JacksonRequestConverterFunction(mapper));
  }

  @Override
  @Nullable
  public Object convertRequest(
      ServiceRequestContext ctx,
      AggregatedHttpRequest request,
      Class<?> expectedResultType,
      @Nullable ParameterizedType expectedParameterizedResultType)
      throws Exception {
    try {
      return delegate.convertRequest(
          ctx, request, expectedResultType, expectedParameterizedResultType);
    } catch (IllegalArgumentException e) {
      if (e.getCause() instanceof JsonProcessingException jpe) {
        throw new MalformedRequestBodyException(jpe);
      }
      throw e;
    }
  }
}
