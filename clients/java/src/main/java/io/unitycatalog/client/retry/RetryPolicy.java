package io.unitycatalog.client.retry;

import java.time.Duration;

public interface RetryPolicy {
  int maxAttempts();

  Duration sleepTime(int attempt);

  boolean allowRetry(int attempt);
}
