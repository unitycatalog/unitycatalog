package io.unitycatalog.server.exception;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class BaseException extends RuntimeException {
  private final ErrorCode errorCode;
  private final String errorMessage;
  private final Throwable cause;
  private final Map<String, String> metadata;

  public BaseException(
      ErrorCode errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
    super(errorMessage, cause);
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.cause = cause;
    this.metadata = metadata;
  }

  public BaseException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    this(errorCode, errorMessage, cause, new HashMap<>());
  }

  public BaseException(ErrorCode errorCode, String errorMessage) {
    this(errorCode, errorMessage, null, new HashMap<>());
  }

  public BaseException(ErrorCode errorCode, Throwable cause) {
    this(errorCode, cause.getMessage(), cause, new HashMap<>());
  }
}
