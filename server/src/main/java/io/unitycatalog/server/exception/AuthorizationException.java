package io.unitycatalog.server.exception;

import java.util.Map;

public class AuthorizationException extends BaseException {
  public AuthorizationException(
      ErrorCode errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
    super(errorCode, errorMessage, cause, metadata);
  }

  public AuthorizationException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode, errorMessage, cause);
  }

  public AuthorizationException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }

  public AuthorizationException(ErrorCode errorCode, Throwable cause) {
    super(errorCode, cause);
  }
}
