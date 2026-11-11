package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.unboundid.scim2.common.exceptions.ScimException;

/**
 * Exception handler for the SCIM2 services. Formats every error as a SCIM Error (RFC 7644 section
 * 3.12) with the HTTP status the error carries.
 */
public final class ScimExceptionHandler extends BaseExceptionHandler {

  /** Shared instance; these handlers are stateless. */
  public static final ScimExceptionHandler INSTANCE = new ScimExceptionHandler();

  private ScimExceptionHandler() {}

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    if (cause instanceof Scim2RuntimeException) {
      ScimException scimException = (ScimException) cause.getCause();
      logIfServerError(ctx, httpStatus(scimException), cause);
      return createErrorResponse(scimException);
    }
    return super.handleException(ctx, req, cause);
  }

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    return createErrorResponse(
        ScimException.createException(httpStatus(exception).code(), exception.getErrorMessage()));
  }

  private static HttpStatus httpStatus(ScimException exception) {
    return HttpStatus.valueOf(exception.getScimError().getStatus());
  }

  private static HttpResponse createErrorResponse(ScimException exception) {
    return HttpResponse.ofJson(httpStatus(exception), exception.getScimError());
  }
}
