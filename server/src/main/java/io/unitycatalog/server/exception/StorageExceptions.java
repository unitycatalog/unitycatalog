package io.unitycatalog.server.exception;

import software.amazon.awssdk.core.exception.SdkException;

/**
 * Failures of the server's own S3 requests. Their messages come from the storage system and name
 * its endpoint, so callers get a general error. The failure is kept as its cause, which the
 * exception handler logs with the response.
 */
public final class StorageExceptions {
  private StorageExceptions() {}

  /** Whether {@code e} or any of its causes is an S3 SDK failure. */
  public static boolean isStorageFailure(Throwable e) {
    for (Throwable current = e; current != null; current = current.getCause()) {
      if (current instanceof SdkException) {
        return true;
      }
      if (current.getCause() == current) {
        break;
      }
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
    return new BaseException(ErrorCode.INTERNAL, "Failed to access storage.", e);
  }
}
