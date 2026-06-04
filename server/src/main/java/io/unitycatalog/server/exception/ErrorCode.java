package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.delta.model.DeltaErrorType;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import lombok.Getter;

/**
 * Error codes for the Unity Catalog server. Each entry is defined as:
 *
 * <pre>
 *   NAME(grpcCode, ucHttpStatus, deltaErrorType)
 *   NAME(grpcCode, ucHttpStatus, deltaErrorType, deltaHttpStatus)
 * </pre>
 *
 * <ul>
 *   <li>{@code grpcCode} - numeric gRPC status code (e.g., 3 = INVALID_ARGUMENT)
 *   <li>{@code ucHttpStatus} - HTTP status code returned by the UC REST API
 *   <li>{@code deltaErrorType} - corresponding {@link DeltaErrorType} in the Delta REST API error
 *       spec
 *   <li>{@code deltaHttpStatus} - HTTP status code for the Delta REST API (defaults to ucHttpStatus
 *       when omitted; overridden when the Delta spec disagrees, e.g., 409 for AlreadyExists vs UC's
 *       legacy 400)
 * </ul>
 */
@Getter
public enum ErrorCode {
  INVALID_ARGUMENT(3, 400, DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION),
  UNSUPPORTED_TABLE_FORMAT(3, 400, DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION),
  NOT_FOUND(5, 404, DeltaErrorType.NOT_FOUND_EXCEPTION),
  CATALOG_NOT_FOUND(5, 404, DeltaErrorType.NO_SUCH_CATALOG_EXCEPTION),
  SCHEMA_NOT_FOUND(5, 404, DeltaErrorType.NO_SUCH_SCHEMA_EXCEPTION),
  TABLE_NOT_FOUND(5, 404, DeltaErrorType.NO_SUCH_TABLE_EXCEPTION),
  ALREADY_EXISTS(6, 409, DeltaErrorType.ALREADY_EXISTS_EXCEPTION),
  PERMISSION_DENIED(7, 403, DeltaErrorType.PERMISSION_DENIED_EXCEPTION),
  UNAUTHENTICATED(16, 401, DeltaErrorType.NOT_AUTHORIZED_EXCEPTION),
  RESOURCE_EXHAUSTED(8, 429, DeltaErrorType.RESOURCE_EXHAUSTED_EXCEPTION),
  FAILED_PRECONDITION(9, 400, DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION),
  // Generic "aborted" code for non-commit callers (e.g. JwksOperations, ModelRepository)
  ABORTED(10, 409, DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION),
  // Dedicated code for CCv2 commit-version replay conflicts.
  COMMIT_VERSION_CONFLICT(10, 409, DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION),
  // Dedicated code for assert-* requirement failures on the Delta update endpoint.
  UPDATE_REQUIREMENT_CONFLICT(9, 409, DeltaErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION),
  OUT_OF_RANGE(11, 400, DeltaErrorType.BAD_REQUEST_EXCEPTION),
  UNIMPLEMENTED(12, 501, DeltaErrorType.NOT_IMPLEMENTED_EXCEPTION),
  INTERNAL(13, 500, DeltaErrorType.INTERNAL_SERVER_ERROR_EXCEPTION),
  DATA_LOSS(15, 500, DeltaErrorType.INTERNAL_SERVER_ERROR_EXCEPTION),
  // UC-specific "already exists" error codes for backwards compatibility (all HTTP 400). But new
  // Delta API returns 409 as it has no backwards compatibility concern.
  RESOURCE_ALREADY_EXISTS(16, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  CATALOG_ALREADY_EXISTS(17, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  SCHEMA_ALREADY_EXISTS(18, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  TABLE_ALREADY_EXISTS(19, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  STORAGE_CREDENTIAL_ALREADY_EXISTS(20, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409),
  EXTERNAL_LOCATION_ALREADY_EXISTS(21, 400, DeltaErrorType.ALREADY_EXISTS_EXCEPTION, 409);

  // Canonical mapping from DeltaErrorType to HTTP status. Built at class-load time and validated
  // to ensure every ErrorCode that shares the same deltaErrorType also agrees on deltaHttpStatus.
  private static final Map<DeltaErrorType, HttpStatus> DELTA_ERROR_TYPE_TO_STATUS;

  static {
    EnumMap<DeltaErrorType, HttpStatus> map = new EnumMap<>(DeltaErrorType.class);
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

  public static HttpStatus getDeltaHttpStatus(DeltaErrorType errorType) {
    HttpStatus status = DELTA_ERROR_TYPE_TO_STATUS.get(errorType);
    if (status == null) {
      throw new IllegalArgumentException("No HTTP status mapped for DeltaErrorType: " + errorType);
    }
    return status;
  }

  /**
   * Looks up the Delta HTTP status by error type value string. Works with both client and server
   * DeltaErrorType enums.
   */
  public static HttpStatus getDeltaHttpStatus(String errorTypeValue) {
    return getDeltaHttpStatus(DeltaErrorType.fromValue(errorTypeValue));
  }

  private final int code;
  private final HttpStatus httpStatus;
  private final DeltaErrorType deltaErrorType;
  // The Delta REST API spec defines its own HTTP status per DeltaErrorType (e.g.,
  // AlreadyExistsException
  // is 409). Some legacy UC error codes use a different status for backward compatibility (e.g.,
  // CATALOG_ALREADY_EXISTS is 400). When the two disagree, deltaHttpStatus carries the spec-correct
  // status for the Delta API; otherwise it defaults to httpStatus.
  private final HttpStatus deltaHttpStatus;

  ErrorCode(int code, int httpStatus, DeltaErrorType deltaErrorType, int deltaHttpStatus) {
    this.code = code;
    this.httpStatus = HttpStatus.valueOf(httpStatus);
    this.deltaErrorType = deltaErrorType;
    this.deltaHttpStatus = HttpStatus.valueOf(deltaHttpStatus);
  }

  ErrorCode(int code, int httpStatus, DeltaErrorType deltaErrorType) {
    this(code, httpStatus, deltaErrorType, httpStatus);
  }
}
