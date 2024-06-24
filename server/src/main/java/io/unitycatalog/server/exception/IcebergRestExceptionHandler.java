package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.utils.RESTObjectMapper;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.responses.ErrorResponse;

public class IcebergRestExceptionHandler implements ExceptionHandlerFunction {
  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    try {
      if (cause instanceof BaseException) {
        return handleBaseException((BaseException)cause);
      } else if(cause instanceof NoSuchTableException) {
        return createErrorResponse(HttpStatus.NOT_FOUND, cause);
      } else {
        // FIXME!!
        return createErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, cause);
      }
    } catch (Exception e) {
      return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private HttpResponse handleBaseException(BaseException exception) {
    return createErrorResponse(exception.getErrorCode().getHttpStatus(), exception);
  }

  @SneakyThrows
  private HttpResponse createErrorResponse(HttpStatus status, Throwable cause) {
    return HttpResponse.of(status, MediaType.JSON, RESTObjectMapper.mapper().writeValueAsString(
      ErrorResponse.builder()
        .responseCode(status.code())
        .withType(cause.getClass().getSimpleName())
        .withMessage(cause.getMessage())
        .build()
    ));
  }

}
