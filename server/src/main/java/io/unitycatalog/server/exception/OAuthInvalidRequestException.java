package io.unitycatalog.server.exception;

public class OAuthInvalidRequestException extends BaseException {
  public OAuthInvalidRequestException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }

  public OAuthInvalidRequestException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode, errorMessage, cause);
  }
}
