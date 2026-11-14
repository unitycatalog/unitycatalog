package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.exceptions.ScimException;
import io.unitycatalog.server.utils.Scim2Utils;
import lombok.SneakyThrows;

/**
 * Exception handler for the SCIM2 services. Formats every error as a SCIM Error (RFC 7644 section
 * 3.12) with the HTTP status the error carries.
 */
public final class ScimExceptionHandler extends BaseExceptionHandler {

  private static final MediaType SCIM_JSON = MediaType.parse("application/scim+json");

  /** Shared instance; these handlers are stateless. */
  public static final ScimExceptionHandler INSTANCE = new ScimExceptionHandler();

  private ScimExceptionHandler() {}

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    if (cause instanceof Scim2RuntimeException) {
      ScimException scimException = (ScimException) cause.getCause();
      // An unsupported operation is the client's doing, not a server failure worth an error log.
      if (httpStatus(scimException) != HttpStatus.NOT_IMPLEMENTED) {
        logIfServerError(ctx, httpStatus(scimException), cause);
      }
      return createErrorResponse(scimException);
    }
    return super.handleException(ctx, req, cause);
  }

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    // A duplicate is a uniqueness conflict (RFC 7644 section 3.12).
    ScimException scimException =
        exception.getErrorCode() == ErrorCode.ALREADY_EXISTS
            ? ResourceConflictException.uniqueness(exception.getErrorMessage())
            : ScimException.createException(
                httpStatus(exception).code(), exception.getErrorMessage());
    return createErrorResponse(scimException);
  }

  private static HttpStatus httpStatus(ScimException exception) {
    return HttpStatus.valueOf(exception.getScimError().getStatus());
  }

  @SneakyThrows
  private static HttpResponse createErrorResponse(ScimException exception) {
    return HttpResponse.of(
        httpStatus(exception),
        SCIM_JSON,
        Scim2Utils.SCIM_MAPPER.writeValueAsBytes(exception.getScimError()));
  }
}
