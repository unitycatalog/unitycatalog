package io.unitycatalog.server.exception;

public class AuthorizationException extends BaseException {
  public AuthorizationException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }
}
