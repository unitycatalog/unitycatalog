package io.unitycatalog.server.exception;

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
    if (cause instanceof BaseException baseException) {
      return switch (baseException.getErrorCode()) {
        case SCHEMA_NOT_FOUND ->
            wrapException(
                ErrorCode.NOT_FOUND,
                baseException.getErrorMessage(),
                new NoSuchNamespaceException("%s", baseException.getErrorMessage()));
        case TABLE_NOT_FOUND ->
            wrapException(
                ErrorCode.NOT_FOUND,
                baseException.getErrorMessage(),
                new NoSuchTableException("%s", baseException.getErrorMessage()));
        case SCHEMA_ALREADY_EXISTS, TABLE_ALREADY_EXISTS ->
            wrapException(
                ErrorCode.ALREADY_EXISTS,
                baseException.getErrorMessage(),
                new AlreadyExistsException("%s", baseException.getErrorMessage()));
        case UPDATE_REQUIREMENT_CONFLICT ->
            wrapException(
                ErrorCode.ALREADY_EXISTS,
                baseException.getErrorMessage(),
                new CommitFailedException("%s", baseException.getErrorMessage()));
        default -> baseException;
      };
    }
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
    return super.toBaseException(cause);
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
