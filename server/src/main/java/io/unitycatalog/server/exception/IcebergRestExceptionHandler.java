package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * Exception handler for the Iceberg REST Catalog API. Formats errors using Iceberg's {@link
 * ErrorResponse}, whose error type is named in Iceberg's own vocabulary (e.g.,
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

  /**
   * Names the error type in the vocabulary the Iceberg REST spec and its clients use.
   *
   * <p>An exception that came from Iceberg already carries such a name, so it is reported as it
   * stands. A {@link BaseException} raised by Unity Catalog itself does not: reporting its class
   * name would put "BaseException" on the wire, which is both an internal name and a type no client
   * recognizes. Iceberg's own client reads the type to tell one 404 from another -- {@code
   * ErrorHandlers.TableErrorHandler} raises {@code NoSuchNamespaceException} only when the type
   * says so, and {@code NoSuchTableException} otherwise -- so an unrecognized type makes a missing
   * catalog or schema surface at the client as a missing table. Those exceptions are named from
   * their error code instead, which is also what the response status is derived from.
   */
  private static String errorType(BaseException exception) {
    Throwable original = getOriginalException(exception);
    if (original instanceof BaseException) {
      return icebergExceptionFor(exception.getErrorCode()).getSimpleName();
    }
    return original.getClass().getSimpleName();
  }

  /**
   * The Iceberg exception that stands for a Unity Catalog error code, chosen as the one Iceberg's
   * client raises for that code's HTTP status; {@code RESTException} covers the statuses the client
   * has no exception of its own for. The switch is exhaustive so that a new error code has to name
   * its Iceberg counterpart rather than silently inherit a misleading one.
   */
  private static Class<? extends Exception> icebergExceptionFor(ErrorCode errorCode) {
    return switch (errorCode) {
      case CATALOG_NOT_FOUND, SCHEMA_NOT_FOUND -> NoSuchNamespaceException.class;
      case TABLE_NOT_FOUND -> NoSuchTableException.class;
      case NOT_FOUND -> NotFoundException.class;
      case ALREADY_EXISTS,
              RESOURCE_ALREADY_EXISTS,
              CATALOG_ALREADY_EXISTS,
              SCHEMA_ALREADY_EXISTS,
              TABLE_ALREADY_EXISTS,
              STORAGE_CREDENTIAL_ALREADY_EXISTS,
              EXTERNAL_LOCATION_ALREADY_EXISTS ->
          AlreadyExistsException.class;
      case ABORTED, COMMIT_VERSION_CONFLICT, UPDATE_REQUIREMENT_CONFLICT ->
          CommitFailedException.class;
      case INVALID_ARGUMENT, UNSUPPORTED_TABLE_FORMAT, FAILED_PRECONDITION, OUT_OF_RANGE ->
          BadRequestException.class;
      case UNAUTHENTICATED -> NotAuthorizedException.class;
      case PERMISSION_DENIED -> ForbiddenException.class;
      case UNIMPLEMENTED -> UnsupportedOperationException.class;
      case INTERNAL, DATA_LOSS -> ServiceFailureException.class;
      case RESOURCE_EXHAUSTED -> RESTException.class;
    };
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
                    .withType(errorType(exception))
                    .withMessage(exception.getErrorMessage())
                    .build()));
  }
}
