package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.unboundid.scim2.common.exceptions.ScimException;
import io.unitycatalog.server.utils.RESTObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public class GlobalExceptionHandler implements ExceptionHandlerFunction {
  @SneakyThrows
  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    if (cause instanceof BaseException) {
      BaseException baseException = (BaseException) cause;
      return HttpResponse.ofJson(
          baseException.getErrorCode().getHttpStatus(),
          createErrorResponse(
              baseException.getErrorCode(),
              baseException.getErrorMessage(),
              baseException.getCause(),
              baseException.getMetadata()));
    } else if (cause instanceof Scim2RuntimeException) {
      ScimException scimException = (ScimException) cause.getCause();
      return HttpResponse.ofJson(
          HttpStatus.INTERNAL_SERVER_ERROR,
          RESTObjectMapper.mapper().writeValueAsString(scimException.getScimError()));
    } else if (cause instanceof RuntimeException) {
      return HttpResponse.ofJson(
          HttpStatus.INTERNAL_SERVER_ERROR,
          createErrorResponse(ErrorCode.INTERNAL, cause.getMessage(), cause, new HashMap<>()));
    }
    return ExceptionHandlerFunction.fallthrough();
  }

  private Map<String, Object> createErrorResponse(
      ErrorCode errorCode, String message, Throwable cause, Map<String, String> metadata) {
    Map<String, Object> response = new HashMap<>();
    response.put("error_code", errorCode.name());
    response.put("message", message);
    response.put("stack_trace", cause != null ? Arrays.toString(cause.getStackTrace()) : null);

    Map<String, Object> details = new HashMap<>();
    details.put("@type", "google.rpc.ErrorInfo");
    details.put("reason", errorCode.name());
    details.put("metadata", metadata);
    response.put("details", List.of(details));

    return response;
  }
}
