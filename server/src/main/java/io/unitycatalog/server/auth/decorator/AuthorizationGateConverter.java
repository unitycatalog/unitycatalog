package io.unitycatalog.server.auth.decorator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.lang.reflect.ParameterizedType;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request-converter wrapper that runs {@link UnityAccessDecorator}'s PAYLOAD-source authorization
 * during body binding, before the handler runs. When a request method reads authorization keys from
 * the body, the decorator stashes a {@link UnityAccessDecorator#PAYLOAD_AUTHORIZER} callback in the
 * request context; this converter delegates to the inner body converter (e.g. Jackson) first and
 * then invokes the callback on the <em>bound object</em>, so authorization sees the exact value the
 * handler will receive rather than a separate parse of the bytes.
 *
 * <p>Failure modes map to distinct statuses, and in neither does the handler run:
 *
 * <ul>
 *   <li>A malformed / missing / non-JSON body fails in the delegate binding (a client error), which
 *       surfaces as {@code 400} before authorization is even attempted.
 *   <li>A well-formed but unauthorized body is denied by the callback, which throws {@code
 *       PERMISSION_DENIED} ({@code 403}).
 * </ul>
 *
 * <p>Wrapping Jackson (rather than sitting in front of it as a separate chain element) means
 * services register a single converter and the gate is impossible to forget when a new annotated
 * service is added.
 *
 * <p>Registered only when authorization is enabled, since it depends on {@link
 * UnityAccessDecorator} to supply the callback; otherwise a plain Jackson converter is used. That
 * pairing is what lets a missing callback be treated as a fault and denied.
 *
 * <p>Scope: runs only for parameters bound via the converter chain. Endpoints with no body-bound
 * parameter (pure {@code @Param}/path) don't trigger this converter at all.
 */
public final class AuthorizationGateConverter implements RequestConverterFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationGateConverter.class);

  private final RequestConverterFunction delegate;
  private final ObjectMapper mapper;

  /**
   * @param delegate the Jackson converter this gate stands in for, which it calls to bind the body
   * @param mapper the mapper {@code delegate} was built from. Passed separately because Jackson's
   *     converter does not expose its own mapper, and it must be the same one so the keys read for
   *     authorization resolve exactly as the handler's binding did.
   */
  public AuthorizationGateConverter(RequestConverterFunction delegate, ObjectMapper mapper) {
    this.delegate = Objects.requireNonNull(delegate);
    this.mapper = Objects.requireNonNull(mapper);
  }

  @Override
  public Object convertRequest(
      ServiceRequestContext ctx,
      AggregatedHttpRequest request,
      Class<?> expectedResultType,
      ParameterizedType expectedParameterizedResultType)
      throws Exception {
    // Bind first, authorize second: a malformed body fails here as a 400, an unauthorized one is
    // denied below as a 403, and the handler runs in neither case. Authorizing the bound object
    // rather than a separate parse means we authorize exactly what the handler receives.
    Object bound =
        delegate.convertRequest(ctx, request, expectedResultType, expectedParameterizedResultType);

    // The access decorator always leaves an authorizer behind, and this gate only exists when that
    // decorator does. A missing one is a broken assumption, not a request needing no check.
    PayloadAuthorizer payloadAuthorizer = ctx.attr(UnityAccessDecorator.PAYLOAD_AUTHORIZER);
    if (payloadAuthorizer == null) {
      LOGGER.error(
          "SECURITY VIOLATION: no payload authorizer was set for {}; the access decorator did not "
              + "run before body binding. Denying the request.",
          ctx.config().route());
      throw new BaseException(
          ErrorCode.PERMISSION_DENIED, UnityAccessDecorator.ERR_AUTH_NOT_EXECUTED);
    }
    // Throws BaseException(PERMISSION_DENIED) to deny; propagates to the exception handler as a
    // 403.
    payloadAuthorizer.authorize(bound, mapper);
    return bound;
  }
}
