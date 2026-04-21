package io.unitycatalog.server.auth.decorator;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.lang.reflect.ParameterizedType;
import java.util.Objects;

/**
 * Fail-closed wrapper that enforces {@link UnityAccessDecorator}'s PAYLOAD-source authorization
 * before delegating to an inner body converter (e.g. Jackson). The decorator sets {@link
 * UnityAccessDecorator#AUTH_PENDING} on entry to the PAYLOAD path and clears it only after {@code
 * checkAuthorization} succeeds; if the flag is still {@code true} when a parameter is being bound,
 * {@code peekData}'s callback never fired (no body, non-JSON content-type, malformed JSON, trailing
 * whitespace) and the request must be denied.
 *
 * <p>Wrapping Jackson (rather than sitting in front of it as a separate chain element) means
 * services register a single converter and the gate is impossible to forget when a new annotated
 * service is added.
 *
 * <p>Scope: gates only parameters bound via the converter chain. Endpoints with no body-bound
 * parameter (pure {@code @Param}/path) don't trigger this converter, but those endpoints also don't
 * use PAYLOAD locators, so the decorator never sets the flag and no gate is needed.
 */
public final class AuthorizationGateConverter implements RequestConverterFunction {

  private final RequestConverterFunction delegate;

  public AuthorizationGateConverter(RequestConverterFunction delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public Object convertRequest(
      ServiceRequestContext ctx,
      AggregatedHttpRequest request,
      Class<?> expectedResultType,
      ParameterizedType expectedParameterizedResultType)
      throws Exception {
    if (Boolean.TRUE.equals(ctx.attr(UnityAccessDecorator.AUTH_PENDING))) {
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED, "Authorization could not be verified for this request");
    }
    return delegate.convertRequest(
        ctx, request, expectedResultType, expectedParameterizedResultType);
  }
}
