package io.unitycatalog.server.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * Exception handler for the Iceberg REST Catalog API. Formats errors using Iceberg's {@link
 * ErrorResponse} with the original exception's class name as the error type (e.g.,
 * "NoSuchTableException"). Uses {@link IcebergObjectMapper} for Iceberg-specific serialization.
 */
public class IcebergRestExceptionHandler extends BaseExceptionHandler {

  /** Shared instance; these handlers are stateless. */
  public static final IcebergRestExceptionHandler INSTANCE = new IcebergRestExceptionHandler();

  private IcebergRestExceptionHandler() {}

  /**
   * Walks the cause chain past BaseException wrappers to find the original exception, so the
   * Iceberg error type reflects the actual exception (e.g., "NoSuchTableException") rather than
   * "BaseException".
   */
  private static Throwable getOriginalException(BaseException exception) {
    Throwable current = exception;
    while (current instanceof BaseException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  @Override
  protected BaseException toBaseException(Throwable cause) {
    if (cause instanceof NoSuchNamespaceException
        || cause instanceof NoSuchTableException
        || cause instanceof NoSuchViewException) {
      return wrapException(ErrorCode.NOT_FOUND, cause);
    }
    if (cause instanceof AlreadyExistsException
        || cause instanceof NamespaceNotEmptyException
        || cause instanceof CommitFailedException) {
      return wrapException(ErrorCode.ALREADY_EXISTS, cause);
    }
    if (cause instanceof BadRequestException) {
      return wrapException(ErrorCode.INVALID_ARGUMENT, cause);
    }
    JsonProcessingException jsonFailure = findJsonFailure(cause);
    if (jsonFailure != null) {
      // A body the request converter cannot read arrives here as a failure whose message names the
      // Jackson exception class and quotes the reader's source location. Report what was wrong with
      // the request instead of the internals of the JSON reader.
      return wrapException(
          ErrorCode.INVALID_ARGUMENT,
          "Malformed request body: " + jsonFailure.getOriginalMessage(),
          cause);
    }
    return super.toBaseException(cause);
  }

  /** The Jackson failure behind a thrown exception, or null if the failure was something else. */
  private static JsonProcessingException findJsonFailure(Throwable cause) {
    for (Throwable current = cause; current != null; current = current.getCause()) {
      if (current instanceof JsonProcessingException jsonProcessingException) {
        return jsonProcessingException;
      }
      if (current.getCause() == current) {
        break;
      }
    }
    return null;
  }

  @SneakyThrows
  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    HttpStatus status = exception.getErrorCode().getHttpStatus();
    return HttpResponse.of(
        status,
        MediaType.JSON,
        IcebergObjectMapper.mapper()
            .writeValueAsString(
                ErrorResponse.builder()
                    .responseCode(status.code())
                    .withType(getOriginalException(exception).getClass().getSimpleName())
                    .withMessage(exception.getErrorMessage())
                    .build()));
  }
}
