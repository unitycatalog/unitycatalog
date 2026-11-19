package io.unitycatalog.server.exception;

import software.amazon.awssdk.core.exception.SdkException;

/**
 * Failures of the server's own AWS SDK calls, such as to S3. Their messages, from the SDK or the
 * storage service, can name its endpoint, so callers get a general error. The failure is kept as
 * the cause, for the caller to log. Azure and GCS failures are not covered.
 */
public final class StorageExceptions {
  /** The message callers get when the server's storage access fails. */
  private static final String GENERAL_MESSAGE = "Failed to access storage.";

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
    return BaseExceptionHandler.wrapException(ErrorCode.INTERNAL, GENERAL_MESSAGE, e);
  }
}
