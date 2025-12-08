package io.unitycatalog.client.retry;

import com.google.common.base.Preconditions;
import java.time.Duration;

/**
 * A retry policy implementation that uses exponential backoff with configurable jitter.
 *
 * <p>This policy implements the {@link RetryPolicy} interface and provides exponential backoff with
 * jitter for retrying failed HTTP requests.
 *
 * <p>The delay for each retry attempt is calculated as: {@code initialDelayMs * multiplier ^
 * (attempt - 1) * (1 ± jitterFactor)}
 *
 * <p>Jitter helps prevent the "thundering herd" problem where many clients retry simultaneously.
 * The jitter factor randomizes the delay within the range {@code [baseDelay * (1 - jitterFactor),
 * baseDelay * (1 + jitterFactor)]}.
 *
 * <p>Default configuration:
 *
 * <ul>
 *   <li>{@link #DEFAULT_MAX_ATTEMPTS}: 3 - maximum attempts per request (includes initial try).
 *   <li>{@link #DEFAULT_INITIAL_DELAY_MS}: 500ms - initial backoff delay.
 *   <li>{@link #DEFAULT_DELAY_MULTIPLIER}: 2.0 - exponential multiplier (e.g., 2.0 doubles the wait
 *       each retry).
 *   <li>{@link #DEFAULT_DELAY_JITTER_FACTOR}: 0.5 - jitter fraction in [0, 1) applied to the
 *       computed delay (e.g., 0.5 randomizes by ±50%).
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy policy = JitterDelayRetryPolicy.builder()
 *     .maxAttempts(5)
 *     .initDelayMs(100L)
 *     .delayMultiplier(2.0)
 *     .delayJitterFactor(0.2)
 *     .build();
 * }</pre>
 *
 * @see RetryPolicy
 */
public class JitterDelayRetryPolicy implements RetryPolicy {
  public static final int DEFAULT_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_INITIAL_DELAY_MS = 500L;
  public static final double DEFAULT_DELAY_MULTIPLIER = 2.0;
  public static final double DEFAULT_DELAY_JITTER_FACTOR = 0.5;

  private final int maxAttempts;
  private final long initialDelayMs;
  private final double delayMultiplier;
  private final double delayJitterFactor;

  private JitterDelayRetryPolicy(
      int maxAttempts, long initialDelayMs, double delayMultiplier, double delayJitterFactor) {
    this.maxAttempts = maxAttempts;
    this.initialDelayMs = initialDelayMs;
    this.delayMultiplier = delayMultiplier;
    this.delayJitterFactor = delayJitterFactor;
  }

  @Override
  public int maxAttempts() {
    return maxAttempts;
  }

  /**
   * Calculates the sleep time before the next retry attempt using exponential backoff with jitter.
   *
   * @param attempt the current attempt number (1-indexed)
   * @return the duration to sleep before retrying
   */
  @Override
  public Duration sleepTime(int attempt) {
    long baseDelay = (long) (initialDelayMs * Math.pow(delayMultiplier, attempt - 1));
    double jitter = (Math.random() - 0.5) * 2 * delayJitterFactor;
    long delay = Math.max(0, (long) (baseDelay * (1 + jitter)));
    return Duration.ofMillis(delay);
  }

  /**
   * Determines whether a retry should be attempted based on the current attempt number.
   *
   * @param attempt the current attempt number (1-indexed)
   * @return true if retry is allowed, false otherwise
   */
  @Override
  public boolean allowRetry(int attempt) {
    return attempt <= maxAttempts;
  }

  /**
   * Creates a new builder for constructing a {@link JitterDelayRetryPolicy}.
   *
   * @return a new builder instance with default values
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating {@link JitterDelayRetryPolicy} instances with custom configuration. */
  public static class Builder {
    private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
    private long initDelayMs = DEFAULT_INITIAL_DELAY_MS;
    private double delayMultiple = DEFAULT_DELAY_MULTIPLIER;
    private double delayJitterFactor = DEFAULT_DELAY_JITTER_FACTOR;

    /**
     * Sets the maximum number of attempts (including the initial request).
     *
     * @param maxAttempts the maximum number of attempts
     * @return this builder instance
     */
    public Builder maxAttempts(int maxAttempts) {
      Preconditions.checkArgument(
          maxAttempts > 0, "maxAttempts must be greater than 0, but got %s", maxAttempts);
      this.maxAttempts = maxAttempts;
      return this;
    }

    /**
     * Sets the initial delay in milliseconds before the first retry.
     *
     * @param initDelayMs the initial delay in milliseconds
     * @return this builder instance
     */
    public Builder initDelayMs(long initDelayMs) {
      Preconditions.checkArgument(
          initDelayMs > 0, "initDelayMs must be greater than 0, but got %s", initDelayMs);
      this.initDelayMs = initDelayMs;
      return this;
    }

    /**
     * Sets the multiplier for exponential backoff.
     *
     * <p>For example, with an initial delay of 100ms and a multiplier of 2.0:
     *
     * <ul>
     *   <li>Attempt 1: 100ms * 2.0^0 = 100ms
     *   <li>Attempt 2: 100ms * 2.0^1 = 200ms
     *   <li>Attempt 3: 100ms * 2.0^2 = 400ms
     * </ul>
     *
     * @param delayMultiplier the exponential backoff multiplier
     * @return this builder instance
     */
    public Builder delayMultiplier(double delayMultiplier) {
      Preconditions.checkArgument(
          delayMultiplier > 0,
          "delayMultiplier must be greater than 0, but got %s",
          delayMultiplier);
      this.delayMultiple = delayMultiplier;
      return this;
    }

    /**
     * Sets the jitter factor to randomize delays and prevent synchronized retries.
     *
     * <p>The jitter factor should be in the range [0, 1). A factor of 0.5 means the actual delay
     * will be randomized between 50% and 150% of the calculated base delay. A factor of 0 disables
     * jitter.
     *
     * @param delayJitterFactor the jitter factor (0 to disable, typically 0.1-0.5)
     * @return this builder instance
     */
    public Builder delayJitterFactor(double delayJitterFactor) {
      Preconditions.checkArgument(
          delayJitterFactor >= 0 && delayJitterFactor < 1,
          "delayJitterFactor must be between 0 and 1, but got %s",
          delayJitterFactor);
      this.delayJitterFactor = delayJitterFactor;
      return this;
    }

    /**
     * Builds a new {@link JitterDelayRetryPolicy} with the configured parameters.
     *
     * @return a new retry policy instance
     */
    public JitterDelayRetryPolicy build() {
      return new JitterDelayRetryPolicy(maxAttempts, initDelayMs, delayMultiple, delayJitterFactor);
    }
  }
}
