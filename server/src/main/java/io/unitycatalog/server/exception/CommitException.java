package io.unitycatalog.server.exception;

import java.util.Map;

public class CommitException extends BaseException {
  public CommitException(
      ErrorCode errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
    super(errorCode, errorMessage, cause, metadata);
  }

  public CommitException(ErrorCode errorCode, String errorMessage, Throwable cause) {
    super(errorCode, errorMessage, cause);
  }

  public CommitException(ErrorCode errorCode, String errorMessage) {
    super(errorCode, errorMessage);
  }

  public CommitException(ErrorCode errorCode, Throwable cause) {
    super(errorCode, cause);
  }
}
