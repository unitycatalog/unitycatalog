package io.unitycatalog.client.retry;

import java.time.Duration;

/**
 * Defines a {@link RetryPolicy} for handling transient failures in Unity Catalog client operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy policy = JitterDelayRetryPolicy.builder().build();
 * int attempt = 1;
 * if (policy.allowRetry(attempt)) {
 *   Thread.sleep(policy.sleepTime(attempt).toMillis());
 *   // Retry the operation
 * }
 * }</pre>
 */
public interface RetryPolicy {
  /**
   * Returns the maximum number of retry attempts allowed by this policy.
   *
   * <p>This value determines the upper bound on retry attempts. A return value of 0 means no
   * retries will be attempted (only the initial attempt), while a positive value indicates the
   * maximum number of additional attempts after the initial failure.
   *
   * @return the maximum number of retry attempts, must be non-negative
   */
  int maxAttempts();

  /**
   * Calculates the sleep duration before the specified retry attempt.
   *
   * <p>This method determines how long to wait before making the next retry attempt. Different
   * implementations may use different strategies such as constant delay, linear backoff, or
   * exponential backoff.
   *
   * @param attempt the current retry attempt number, starting from 1 for the first retry
   * @return the duration to sleep before this retry attempt, must not be null or negative
   * @throws IllegalArgumentException if attempt is less than 1 or greater than maxAttempts()
   */
  Duration sleepTime(int attempt);

  /**
   * Determines whether a retry should be allowed for the given attempt number.
   *
   * <p>This method is invoked to check if another retry attempt should be made based on the current
   * attempt number and the policy's configuration. Typically returns {@code false} when the attempt
   * number exceeds {@link #maxAttempts()}.
   *
   * @param attempt the current retry attempt number, starting from 1 for the first retry
   * @return {@code true} if a retry is allowed for this attempt, {@code false} otherwise
   */
  boolean allowRetry(int attempt);
}
