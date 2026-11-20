package io.unitycatalog.client.retry;

import java.time.Duration;

/**
 * Defines a {@link RetryPolicy} for handling transient failures in Unity Catalog client operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy policy = JitterDelayRetryPolicy.builder().build();
 * for(int attempt = 1; policy.allowRetry(attempt); attempt +=1) {
 *   try {
 *     doSomeAction();
 *   } catch (Exception e) {
 *     if (isRetryable(e) && attempt < policy.maxAttempts()) {
 *       Thread.sleep(policy.sleepTime(attempt).toMillis());
 *     } else {
 *       throw e;
 *     }
 *   }
 * }
 * }</pre>
 */
public interface RetryPolicy {
  /**
   * Returns the maximum number of attempts allowed by this policy.
   *
   * <p>This value is the total number of attempts, including the initial one, so a value of 1 means
   * no retries. No backoff is slept after the last attempt.
   *
   * @return the maximum number of attempts, must be positive
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
   * attempt number and the policy's configuration. Typically, returns {@code false} when the
   * attempt number exceeds {@link #maxAttempts()}.
   *
   * @param attempt the current retry attempt number, starting from 1 for the first retry
   * @return {@code true} if a retry is allowed for this attempt, {@code false} otherwise
   */
  boolean allowRetry(int attempt);
}
