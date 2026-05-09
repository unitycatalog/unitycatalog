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
 * dedicated {@link MalformedRequestBodyException}. The goal is a type-precise signal for {@link
 * BaseExceptionHandler}: body-parse failures become a distinct class nothing else throws, so
 * mapping them to 400 does not require matching on {@link IllegalArgumentException} (which also
 * fires for typed {@code @Param} conversion and for application-code validation).
 *
 * <p>{@code JacksonRequestConverterFunction} is {@code final}, so we delegate rather than
 * subclass. The catch is narrowed to the exact shape Armeria's Jackson wrapper produces:
 * {@code IllegalArgumentException} with a {@link JsonProcessingException} cause. Any other
 * exception (including Armeria's {@code FallthroughException}, a plain {@code RuntimeException})
 * propagates unchanged.
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
