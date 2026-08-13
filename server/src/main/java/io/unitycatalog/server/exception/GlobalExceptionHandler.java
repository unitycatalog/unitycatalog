package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.unboundid.scim2.common.exceptions.ScimException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exception handler for the main Unity Catalog REST API. Formats errors as JSON with error_code,
 * message, stack_trace, and google.rpc.ErrorInfo details.
 */
public class GlobalExceptionHandler extends BaseExceptionHandler {

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    // SCIM has its own error format, handle before normalization
    if (cause instanceof Scim2RuntimeException) {
      ScimException scimException = (ScimException) cause.getCause();
      HttpStatus httpStatus = HttpStatus.valueOf(scimException.getScimError().getStatus());
      return HttpResponse.ofJson(httpStatus, scimException.getScimError());
    }
    return super.handleException(ctx, req, cause);
  }

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    Map<String, Object> response = new HashMap<>();
    response.put("error_code", exception.getErrorCode().name());
    response.put("message", exception.getErrorMessage());
    if (isIncludeStackTrace()) {
      response.put("stack_trace", Arrays.toString(getRelevantStackTrace(exception)));
    }

    Map<String, Object> details = new HashMap<>();
    details.put("@type", "google.rpc.ErrorInfo");
    details.put("reason", exception.getErrorCode().name());
    response.put("details", List.of(details));

    return HttpResponse.ofJson(exception.getErrorCode().getHttpStatus(), response);
  }
}
