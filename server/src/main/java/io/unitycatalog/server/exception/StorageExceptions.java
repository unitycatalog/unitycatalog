package io.unitycatalog.server.exception;

import software.amazon.awssdk.core.exception.SdkException;

/**
 * Failures of the server's own AWS SDK calls, such as to S3 or STS. Their messages come from the
 * storage or STS service and can name its endpoint, so callers get a general error. The failure is
 * kept as the cause, for the caller to log. Azure and GCS failures are not covered.
 */
public final class StorageExceptions {
  /** The message callers get when the server's storage access fails. */
  public static final String GENERAL_MESSAGE = "Failed to access storage.";

  // Bounds the walk over causes, which a cycle would otherwise make endless.
  private static final int MAX_CAUSE_DEPTH = 64;

  private StorageExceptions() {}

  /**
   * Whether {@code e} or one of its causes is an AWS SDK failure. The walk stops at a {@link
   * BaseException}, which already says what callers get.
   */
  public static boolean isStorageFailure(Throwable e) {
    Throwable current = e;
    for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (current instanceof SdkException) {
        return true;
      }
      if (current instanceof BaseException) {
        return false;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Returns the general error reported for a storage failure.
   *
   * @param e a failure for which {@link #isStorageFailure} holds
   * @return an INTERNAL error that carries no storage detail in its message
   */
  public static BaseException toGeneralError(Throwable e) {
    BaseException error = new BaseException(ErrorCode.INTERNAL, GENERAL_MESSAGE, e);
    // As BaseExceptionHandler.wrapException does: error responses then show the cause's trace.
    error.setStackTrace(new StackTraceElement[0]);
    return error;
  }
}
