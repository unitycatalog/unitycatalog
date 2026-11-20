package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Exception handler for the main Unity Catalog REST API. Formats errors as JSON with error_code,
 * message, stack_trace, and google.rpc.ErrorInfo details.
 */
public class GlobalExceptionHandler extends BaseExceptionHandler {

  /** Shared instance; these handlers are stateless. */
  public static final GlobalExceptionHandler INSTANCE = new GlobalExceptionHandler();

  private GlobalExceptionHandler() {}

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    Map<String, Object> response = new HashMap<>();
    response.put("error_code", exception.getErrorCode().name());
    response.put("message", exception.getErrorMessage());
    if (isIncludeStackTrace()) {
      response.put("stack_trace", Arrays.toString(getRelevantStackTrace(exception)));
    }

    Map<String, Object> details = new HashMap<>();
    details.put("@type", "type.googleapis.com/google.rpc.ErrorInfo");
    details.put("reason", exception.getErrorCode().name());
    response.put("details", List.of(details));

    return HttpResponse.ofJson(httpStatus(exception), response);
  }
}
