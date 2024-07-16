package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpStatus;
import lombok.Getter;

@Getter
public enum ErrorCode {
  OK(0, 200),
  CANCELLED(1, 499),
  UNKNOWN(2, 500),
  INVALID_ARGUMENT(3, 400),
  DEADLINE_EXCEEDED(4, 504),
  NOT_FOUND(5, 404),
  ALREADY_EXISTS(6, 409),
  PERMISSION_DENIED(7, 403),
  UNAUTHENTICATED(16, 401),
  RESOURCE_EXHAUSTED(8, 429),
  FAILED_PRECONDITION(9, 400),
  ABORTED(10, 409),
  OUT_OF_RANGE(11, 400),
  UNIMPLEMENTED(12, 501),
  INTERNAL(13, 500),
  UNAVAILABLE(14, 503),
  DATA_LOSS(15, 500);

  private final int code;
  private final HttpStatus httpStatus;

  ErrorCode(int code, int httpStatus) {
    this.code = code;
    this.httpStatus = HttpStatus.valueOf(httpStatus);
  }
}
