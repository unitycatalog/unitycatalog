package io.unitycatalog.server.exception;

import java.util.Map;

public class OAuthInvalidRequestException extends BaseException {
  public OAuthInvalidRequestException(
      ErrorCode errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
    super(errorCode, errorMessage, cause, metadata);
  }

  public OAuthInvalidRequestException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode, errorMessage, cause);
  }

  public OAuthInvalidRequestException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }

  public OAuthInvalidRequestException(ErrorCode errorCode, Throwable cause) {
    super(errorCode, cause);
  }
}
