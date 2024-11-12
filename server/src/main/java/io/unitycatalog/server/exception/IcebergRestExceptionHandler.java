package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.utils.RESTObjectMapper;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.responses.ErrorResponse;

public class IcebergRestExceptionHandler implements ExceptionHandlerFunction {
  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    try {
      if (cause instanceof BaseException) {
        // FIXME!! we should probably translate BaseException -> Iceberg REST exception somewhere
        return handleBaseException((BaseException) cause);
      } else if (cause instanceof NoSuchNamespaceException
          || cause instanceof NoSuchTableException
          || cause instanceof NoSuchViewException) {
        return createErrorResponse(HttpStatus.NOT_FOUND, cause);
      } else if (cause instanceof AlreadyExistsException
          || cause instanceof NamespaceNotEmptyException
          || cause instanceof CommitFailedException) {
        return createErrorResponse(HttpStatus.CONFLICT, cause);
      } else if (cause instanceof IllegalArgumentException
          || cause instanceof BadRequestException) {
        return createErrorResponse(HttpStatus.BAD_REQUEST, cause);
      } else {
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
    return HttpResponse.of(
        status,
        MediaType.JSON,
        RESTObjectMapper.mapper()
            .writeValueAsString(
                ErrorResponse.builder()
                    .responseCode(status.code())
                    .withType(cause.getClass().getSimpleName())
                    .withMessage(cause.getMessage())
                    .build()));
  }
}
