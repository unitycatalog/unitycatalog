package io.unitycatalog.client.retry;

import java.time.Duration;

public class FixedDelayRetryPolicy implements RetryPolicy {
  private final int maxAttempts;
  private final Duration delay;

  private FixedDelayRetryPolicy(int maxAttempts, Duration delay) {
    this.maxAttempts = maxAttempts;
    this.delay = delay;
  }

  @Override
  public int maxAttempts() {
    return maxAttempts;
  }

  @Override
  public Duration sleepTime(int attempt) {
    return delay;
  }

  @Override
  public boolean allowRetry(int attempt) {
    return attempt <= maxAttempts;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int maxAttempts;
    private Duration delay;

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder delay(Duration delay) {
      this.delay = delay;
      return this;
    }

    public FixedDelayRetryPolicy build() {
      return new FixedDelayRetryPolicy(maxAttempts, delay);
    }
  }
}
