package io.unitycatalog.spark;

import org.sparkproject.guava.base.Preconditions;

/**
 * Holds configuration that tweaks the behaviour of the Unity Catalog API client.
 *
 * <p>The defaults here are chosen to work out of the box. Callers can override them either
 * programmatically or via {@link UCHadoopConf#setApiClientConf} when communicating through
 * Hadoop configuration.</p>
 */
public class ApiClientConf {

  public static final int DEFAULT_REQUEST_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_REQUEST_INITIAL_DELAY_MS = 500L;
  public static final double DEFAULT_REQUEST_MULTIPLIER = 2.0;
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

