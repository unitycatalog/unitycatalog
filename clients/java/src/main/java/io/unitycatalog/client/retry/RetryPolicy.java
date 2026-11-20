package io.unitycatalog.client.retry;

import java.time.Duration;

/**
 * Defines a {@link RetryPolicy} for handling transient failures in Unity Catalog client operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy policy = JitterDelayRetryPolicy.builder().build();
 * for (int attempt = 1; attempt <= policy.maxAttempts(); attempt += 1) {
 *   try {
 *     doSomeAction();
 *     return;
 *   } catch (Exception e) {
 *     if (isRetryable(e) && attempt < policy.maxAttempts() && policy.allowRetry(attempt)) {
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
   * Calculates the sleep duration before the attempt that follows the given failed attempt.
   *
   * <p>This method determines how long to wait before making the next retry attempt. Different
   * implementations may use different strategies such as constant delay, linear backoff, or
   * exponential backoff.
   *
   * @param attempt the number of the attempt that just failed, from 1 to {@code maxAttempts() - 1}
   * @return the duration to sleep before the next attempt, must not be null or negative
   */
  Duration sleepTime(int attempt);

  /**
   * Determines whether a retry is allowed for the given attempt number.
   *
   * <p>The client's retry handler asks it after an attempt fails, and only while fewer than {@link
   * #maxAttempts()} attempts have been made, so a policy can return {@code false} to stop retrying
   * earlier.
   *
   * @param attempt the attempt number, starting from 1
   * @return {@code true} if a retry is allowed for this attempt, {@code false} otherwise
   */
  boolean allowRetry(int attempt);
}
