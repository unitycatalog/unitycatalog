package io.unitycatalog.server.exception;

public class OAuthInvalidClientException extends BaseException {
  public OAuthInvalidClientException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }
}
