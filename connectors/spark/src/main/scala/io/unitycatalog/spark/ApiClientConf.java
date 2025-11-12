package io.unitycatalog.spark;

import org.sparkproject.guava.base.Preconditions;

/**
 * Holds configuration that tweaks the behaviour of the Unity Catalog API client.
 *
 * <p>The defaults here are chosen to work out of the box. Callers can override them either
 * programmatically or via {@link UCHadoopConf#setApiClientConf} when communicating through
 * Hadoop configuration. These settings are used by {@link RetryingApiClient} to configure the
 * retry behaviour of the {@link RetryingHttpClient}.</p>
 *
 * <p>Retry defaults:</p>
 * <ul>
 *   <li>{@link #DEFAULT_REQUEST_MAX_ATTEMPTS}: maximum attempts per request (initial try plus
 *       retries).</li>
 *   <li>{@link #DEFAULT_REQUEST_INITIAL_DELAY_MS}: initial backoff delay in milliseconds; later
 *       attempts scale this using {@code initialDelayMs * multiplier ^ (attempt - 1) *
 *       (1 ± jitterFactor)}.</li>
 *   <li>{@link #DEFAULT_REQUEST_DELAY_MULTIPLIER}: exponential multiplier (e.g. {@code 2.0} doubles
 *       the wait each retry).</li>
 *   <li>{@link #DEFAULT_REQUEST_DELAY_JITTER_FACTOR}: jitter fraction in {@code [0, 1)} applied to
 *       the computed delay (e.g. {@code 0.5} randomises by ±50%).</li>
 * </ul>
 */
public class ApiClientConf {

  public static final int DEFAULT_REQUEST_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_REQUEST_INITIAL_DELAY_MS = 500L;
  public static final double DEFAULT_REQUEST_DELAY_MULTIPLIER = 2.0;
  public static final double DEFAULT_REQUEST_DELAY_JITTER_FACTOR = 0.5;

  private int requestMaxAttempts;
  private long requestInitialDelayMs;
  private double requestDelayMultiplier;
  private double requestDelayJitterFactor;

  public ApiClientConf() {
    this.requestMaxAttempts = DEFAULT_REQUEST_MAX_ATTEMPTS;
    this.requestInitialDelayMs = DEFAULT_REQUEST_INITIAL_DELAY_MS;
    this.requestDelayMultiplier = DEFAULT_REQUEST_DELAY_MULTIPLIER;
    this.requestDelayJitterFactor = DEFAULT_REQUEST_DELAY_JITTER_FACTOR;
  }

  public int getRequestMaxAttempts() {
    return requestMaxAttempts;
  }

  public ApiClientConf setRequestMaxAttempts(int requestMaxAttempts) {
    Preconditions.checkArgument(requestMaxAttempts >= 1,
        "Retry max attempts must be at least 1, but got %s", requestMaxAttempts);
    this.requestMaxAttempts = requestMaxAttempts;
    return this;
  }

  public long getRequestInitialDelayMs() {
    return requestInitialDelayMs;
  }

  public ApiClientConf setRequestInitialDelayMs(long requestInitialDelayMs) {
    Preconditions.checkArgument(requestInitialDelayMs > 0,
        "Retry initial delay must be positive, but got %s", requestInitialDelayMs);
    this.requestInitialDelayMs = requestInitialDelayMs;
    return this;
  }

  public double getRequestDelayMultiplier() {
    return requestDelayMultiplier;
  }

  public ApiClientConf setRequestDelayMultiplier(double requestDelayMultiplier) {
    Preconditions.checkArgument(requestDelayMultiplier > 0,
        "Retry delay multiplier must be positive, but got %s", requestDelayMultiplier);
    this.requestDelayMultiplier = requestDelayMultiplier;
    return this;
  }

  public double getRequestDelayJitterFactor() {
    return requestDelayJitterFactor;
  }

  public ApiClientConf setRequestDelayJitterFactor(double requestDelayJitterFactor) {
    Preconditions.checkArgument(requestDelayJitterFactor >= 0 && requestDelayJitterFactor < 1,
        "Retry delay jitter factor must be in [0, 1), but got %s", requestDelayJitterFactor);
    this.requestDelayJitterFactor = requestDelayJitterFactor;
    return this;
  }
}

