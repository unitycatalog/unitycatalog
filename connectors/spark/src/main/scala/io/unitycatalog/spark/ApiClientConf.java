package io.unitycatalog.spark;

import org.sparkproject.guava.base.Preconditions;

/**
 * Holds configuration that tweaks the behaviour of the Unity Catalog API client.
 *
 * <p>The defaults here are chosen to work out of the box. Callers can override them either
 * programmatically or via {@link UCHadoopConf#setApiClientConf} when communicating through
 * Hadoop configuration. These settings are used by {@link RetryingApiClient} to configure the
 * retry behaviour of the {@link RetryingHttpClient}.</p>
 */
public class ApiClientConf {

  // Default maximum attempts per request (initial try + retries).
  public static final int DEFAULT_REQUEST_MAX_ATTEMPTS = 3;
  // Default initial backoff delay, in milliseconds. This is the wait time before the second
  // attempt. Later attempts scale this delay exponentially with the formula:
  // delay = initialDelayMs * multiplier ^ (attempt - 1) * (1 ± jitterFactor).
  public static final long DEFAULT_REQUEST_INITIAL_DELAY_MS = 500L;
  // Default exponential backoff multiplier. Each retry multiplies the previous delay by this
  // factor (e.g. 2.0 doubles the wait).
  public static final double DEFAULT_REQUEST_MULTIPLIER = 2.0;
  // Default jitter factor expressed as a fraction of the calculated delay. A value of 0.5 means we
  // randomise by ±50% around the base backoff.
  public static final double DEFAULT_REQUEST_JITTER_FACTOR = 0.5;

  private int requestMaxAttempts;
  private long requestInitialDelayMs;
  private double requestMultiplier;
  private double requestJitterFactor;

  public ApiClientConf() {
    this.requestMaxAttempts = DEFAULT_REQUEST_MAX_ATTEMPTS;
    this.requestInitialDelayMs = DEFAULT_REQUEST_INITIAL_DELAY_MS;
    this.requestMultiplier = DEFAULT_REQUEST_MULTIPLIER;
    this.requestJitterFactor = DEFAULT_REQUEST_JITTER_FACTOR;
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

  public double getRequestMultiplier() {
    return requestMultiplier;
  }

  public ApiClientConf setRequestMultiplier(double requestMultiplier) {
    Preconditions.checkArgument(requestMultiplier > 0,
        "Retry multiplier must be positive, but got %s", requestMultiplier);
    this.requestMultiplier = requestMultiplier;
    return this;
  }

  public double getRequestJitterFactor() {
    return requestJitterFactor;
  }

  public ApiClientConf setRequestJitterFactor(double requestJitterFactor) {
    Preconditions.checkArgument(requestJitterFactor >= 0 && requestJitterFactor < 1,
        "Retry jitter factor must be in [0, 1), but got %s", requestJitterFactor);
    this.requestJitterFactor = requestJitterFactor;
    return this;
  }
}

