package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.delta.model.ErrorType;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import lombok.Getter;

@Getter
public enum ErrorCode {
  INVALID_ARGUMENT(3, 400, ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION),
  NOT_FOUND(5, 404, ErrorType.NOT_FOUND_EXCEPTION),
  CATALOG_NOT_FOUND(5, 404, ErrorType.NO_SUCH_CATALOG_EXCEPTION),
  SCHEMA_NOT_FOUND(5, 404, ErrorType.NO_SUCH_SCHEMA_EXCEPTION),
  TABLE_NOT_FOUND(5, 404, ErrorType.NO_SUCH_TABLE_EXCEPTION),
  ALREADY_EXISTS(6, 409, ErrorType.ALREADY_EXISTS_EXCEPTION),
  PERMISSION_DENIED(7, 403, ErrorType.PERMISSION_DENIED_EXCEPTION),
  UNAUTHENTICATED(16, 401, ErrorType.NOT_AUTHORIZED_EXCEPTION),
  RESOURCE_EXHAUSTED(8, 429, ErrorType.RESOURCE_EXHAUSTED_EXCEPTION),
  FAILED_PRECONDITION(9, 400, ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION),
  ABORTED(10, 409, ErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION),
  OUT_OF_RANGE(11, 400, ErrorType.BAD_REQUEST_EXCEPTION),
  UNIMPLEMENTED(12, 501, ErrorType.NOT_IMPLEMENTED_EXCEPTION),
  INTERNAL(13, 500, ErrorType.INTERNAL_SERVER_ERROR_EXCEPTION),
  DATA_LOSS(15, 500, ErrorType.INTERNAL_SERVER_ERROR_EXCEPTION),
  // UC-specific "already exists" error codes for backwards compatibility (all HTTP 400). But new
  // Delta API returns 409 as it has no backwards compatibility concern.
  RESOURCE_ALREADY_EXISTS(16, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  CATALOG_ALREADY_EXISTS(17, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  SCHEMA_ALREADY_EXISTS(18, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  TABLE_ALREADY_EXISTS(19, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  STORAGE_CREDENTIAL_ALREADY_EXISTS(20, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  EXTERNAL_LOCATION_ALREADY_EXISTS(21, 400, ErrorType.ALREADY_EXISTS_EXCEPTION, 409);

  // Canonical mapping from Delta ErrorType to HTTP status. Built at class-load time and validated
  // to ensure every ErrorCode that shares the same deltaErrorType also agrees on deltaHttpStatus.
  private static final Map<ErrorType, HttpStatus> DELTA_ERROR_TYPE_TO_STATUS;

  static {
    EnumMap<ErrorType, HttpStatus> map = new EnumMap<>(ErrorType.class);
    for (ErrorCode ec : values()) {
      HttpStatus existing = map.put(ec.deltaErrorType, ec.deltaHttpStatus);
      if (existing != null && !existing.equals(ec.deltaHttpStatus)) {
        throw new IllegalStateException(
            String.format(
                "Conflicting deltaHttpStatus for %s: %s uses %s but a previous entry uses %s",
                ec.deltaErrorType, ec.name(), ec.deltaHttpStatus, existing));
      }
    }
    DELTA_ERROR_TYPE_TO_STATUS = Collections.unmodifiableMap(map);
  }

  public static HttpStatus getDeltaHttpStatus(ErrorType errorType) {
    HttpStatus status = DELTA_ERROR_TYPE_TO_STATUS.get(errorType);
    if (status == null) {
      throw new IllegalArgumentException("No HTTP status mapped for ErrorType: " + errorType);
    }
    return status;
  }

  /**
   * Looks up the Delta HTTP status by error type value string. Works with both client and server
   * ErrorType enums.
   */
  public static HttpStatus getDeltaHttpStatus(String errorTypeValue) {
    return getDeltaHttpStatus(ErrorType.fromValue(errorTypeValue));
  }

  private final int code;
  private final HttpStatus httpStatus;
  private final ErrorType deltaErrorType;
  // The Delta REST API spec defines its own HTTP status per ErrorType (e.g., AlreadyExistsException
  // is 409). Some legacy UC error codes use a different status for backward compatibility (e.g.,
  // CATALOG_ALREADY_EXISTS is 400). When the two disagree, deltaHttpStatus carries the spec-correct
  // status for the Delta API; otherwise it defaults to httpStatus.
  private final HttpStatus deltaHttpStatus;

  ErrorCode(int code, int httpStatus, ErrorType deltaErrorType, int deltaHttpStatus) {
    this.code = code;
    this.httpStatus = HttpStatus.valueOf(httpStatus);
    this.deltaErrorType = deltaErrorType;
    this.deltaHttpStatus = HttpStatus.valueOf(deltaHttpStatus);
  }

  ErrorCode(int code, int httpStatus, ErrorType deltaErrorType) {
    this(code, httpStatus, deltaErrorType, httpStatus);
  }
}
