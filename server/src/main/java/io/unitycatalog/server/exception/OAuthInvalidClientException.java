package io.unitycatalog.server.exception;

import java.util.Map;

public class OAuthInvalidClientException extends BaseException {
  public OAuthInvalidClientException(
      ErrorCode errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
    super(errorCode, errorMessage, cause, metadata);
  }

  public OAuthInvalidClientException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode, errorMessage, cause);
  }

  public OAuthInvalidClientException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }

  public OAuthInvalidClientException(ErrorCode errorCode, Throwable cause) {
    super(errorCode, cause);
  }
}
