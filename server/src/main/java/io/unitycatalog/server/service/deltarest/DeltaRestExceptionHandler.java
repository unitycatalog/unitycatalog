package io.unitycatalog.server.service.deltarest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.exception.BaseException;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception handler that converts exceptions to the Delta REST Catalog error response format.
 *
 * <p>The error response format follows the delta-rest.yaml ErrorResponse schema:
 *
 * <pre>
 * {
 *   "error": {
 *     "message": "...",
 *     "type": "...",
 *     "code": 4xx/5xx
 *   }
 * }
 * </pre>
 */
public class DeltaRestExceptionHandler implements ExceptionHandlerFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaRestExceptionHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  @SneakyThrows
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    try {
      if (cause instanceof BaseException baseException) {
        return createErrorResponse(
            baseException.getErrorCode().getHttpStatus(),
            cause.getClass().getSimpleName(),
            baseException.getErrorMessage());
      } else if (cause instanceof IllegalArgumentException) {
        return createErrorResponse(
            HttpStatus.BAD_REQUEST, "BadRequestException", cause.getMessage());
      } else {
        LOGGER.error("Unhandled exception in Delta REST API", cause);
        return createErrorResponse(
            HttpStatus.INTERNAL_SERVER_ERROR, "InternalServerError", cause.getMessage());
      }
    } catch (Exception e) {
      LOGGER.error("Error handling exception", e);
      return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @SneakyThrows
  private HttpResponse createErrorResponse(HttpStatus status, String type, String message) {
    Map<String, Object> errorModel = new LinkedHashMap<>();
    errorModel.put("message", message);
    errorModel.put("type", type);
    errorModel.put("code", status.code());

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("error", errorModel);

    return HttpResponse.of(status, MediaType.JSON, MAPPER.writeValueAsString(response));
  }
}
