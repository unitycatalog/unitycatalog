package io.unitycatalog.spark;

import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.base.Throwables;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * Default implementation of {@link HttpRetryHandler} that retries on common
 * recoverable HTTP errors and network exceptions.
 */
public class DefaultHttpRetryHandler implements HttpRetryHandler {
  // Non-5xx server errors are not retried.
  private static final Set<Integer> RECOVERABLE_STATUS_CODES = Set.of(
      429  // Too Many Requests
  );

  private static final Set<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS = Set.of(
      java.net.SocketTimeoutException.class,
      java.net.SocketException.class,
      java.net.UnknownHostException.class
  );

  private final Clock clock;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;
  private final double jitterFactor;

  public DefaultHttpRetryHandler(Configuration conf, Clock clock) {
    this.clock = clock;
    this.maxAttempts = conf.getInt(
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY,
        UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT);
    this.initialDelayMs = conf.getLong(
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY,
        UCHadoopConf.RETRY_INITIAL_DELAY_DEFAULT);
    this.multiplier = conf.getDouble(
        UCHadoopConf.RETRY_MULTIPLIER_KEY,
        UCHadoopConf.RETRY_MULTIPLIER_DEFAULT);
    this.jitterFactor = conf.getDouble(
        UCHadoopConf.RETRY_JITTER_FACTOR_KEY,
        UCHadoopConf.RETRY_JITTER_FACTOR_DEFAULT);

    Preconditions.checkArgument(maxAttempts >= 1,
        "Retry max attempts must be at least 1, but got '%s'=%s",
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, maxAttempts);
    Preconditions.checkArgument(initialDelayMs > 0,
        "Retry initial delay must be positive, but got '%s'=%s",
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY, initialDelayMs);
    Preconditions.checkArgument(multiplier > 0,
        "Retry multiplier must be positive, but got '%s'=%s",
        UCHadoopConf.RETRY_MULTIPLIER_KEY, multiplier);
    Preconditions.checkArgument(jitterFactor >= 0 && jitterFactor < 1,
        "Retry jitter factor must be between 0 and 1 (exclusive), but got '%s'=%s",
        UCHadoopConf.RETRY_JITTER_FACTOR_KEY, jitterFactor);
  }

  @Override
  public <T> HttpResponse<T> call(
      HttpClient delegate,
      HttpRequest request,
      HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
    IOException lastException = null;
    Instant startTime = clock.now();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      HttpResponse<T> response;
      boolean shouldRetry = true;
      try {
        response = delegate.send(request, responseBodyHandler);
        if(!isRetryable(response.statusCode())){
          return response;
        }
      } catch (IOException e) {
        lastException = e;
        shouldRetry = isRecoverableException(e);
      }

      if(shouldRetry && attempt < maxAttempts){
        sleepWithBackoff(attempt);
      } else {
        break;
      }
    }

    if(lastException != null) {
      throw lastException;
    } else {
      long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
      throw new IOException(String.format( "Failed HTTP request after %s attempts" +
          " with elapsed time %s ms", maxAttempts, elapsedMs));
    }
  }

  private static boolean isRetryable(int statusCode) {
    return RECOVERABLE_STATUS_CODES.contains(statusCode) ||
        (statusCode >= 500 && statusCode < 600);
  }

  private void sleepWithBackoff(int attempt) throws InterruptedException {
    long baseDelay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
    double jitter = jitterFactor == 0 ? 0 : (Math.random() - 0.5) * 2 * jitterFactor;
    long delay = (long) (baseDelay * (1 + jitter));
    if (delay <= 0) {
      return;
    }

    try {
      clock.sleep(Duration.ofMillis(delay));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  private boolean isRecoverableException(Throwable e) {
    // Check the entire exception cause chain for recoverable exceptions
    return Throwables.getCausalChain(e).stream()
        .anyMatch(cause -> RECOVERABLE_EXCEPTIONS.stream()
            .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
  }
}

