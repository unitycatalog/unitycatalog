package io.unitycatalog.client;

public class RetryPolicy {
  private final int maxAttempts;
  private final long initialDelayMillis;
  private final double delayMultiplier;
  private final double delayJitterFactor;

  private RetryPolicy(
      int maxAttempts,
      long initialDelayMillis,
      double delayMultiplier,
      double delayJitterFactor) {
    this.maxAttempts = maxAttempts;
    this.initialDelayMillis = initialDelayMillis;
    this.delayMultiplier = delayMultiplier;
    this.delayJitterFactor = delayJitterFactor;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private int maxAttempts = 3;
    private long initialDelayMillis = 500L;
    private double delayMultiplier = 2.0;
    private double delayJitterFactor = 0.5;

    public Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder initialDelayMillis(long initialDelayMillis) {
      this.initialDelayMillis = initialDelayMillis;
      return this;
    }

    public Builder delayMultiplier(double delayMultiplier) {
      this.delayMultiplier = delayMultiplier;
      return this;
    }

    public Builder delayJitterFactor(double delayJitterFactor) {
      this.delayJitterFactor = delayJitterFactor;
      return this;
    }

    public RetryPolicy build() {
      return new RetryPolicy(maxAttempts, initialDelayMillis, delayMultiplier, delayJitterFactor);
    }
  }
}
