package io.unitycatalog.server.exception;

import lombok.Getter;

@Getter
public class BaseException extends RuntimeException {
  private final ErrorCode errorCode;
  private final String errorMessage;
  private final Throwable cause;

  public BaseException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorMessage, cause);
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.cause = cause;
  }

  public BaseException(ErrorCode errorCode, String errorMessage) {
    this(errorCode, errorMessage, null);
  }
}
