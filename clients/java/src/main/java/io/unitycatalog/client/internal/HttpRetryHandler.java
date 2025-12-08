package io.unitycatalog.client.internal;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.unitycatalog.client.retry.RetryPolicy;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/** Retry handler that retries on common recoverable HTTP errors and network exceptions. */
class HttpRetryHandler {
  // Non-5xx server errors are not retried.
  private static final Set<Integer> RECOVERABLE_STATUS_CODES =
      Set.of(
          429 // Too Many Requests
          );

  private static final Set<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS =
      Set.of(
          java.net.SocketTimeoutException.class,
          java.net.SocketException.class,
          java.net.UnknownHostException.class);

  private final Clock clock;
  private final RetryPolicy retryPolicy;

  HttpRetryHandler(RetryPolicy retryPolicy, Clock clock) {
    Preconditions.checkNotNull(retryPolicy, "RetryPolicy must not be null");
    Preconditions.checkNotNull(clock, "Clock must not be null");
    this.clock = clock;
    this.retryPolicy = retryPolicy;
  }

  public <T> HttpResponse<T> call(
      HttpClient delegate, HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
      throws IOException, InterruptedException {
    IOException lastException = null;
    Instant startTime = clock.now();

    for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
      HttpResponse<T> response;
      boolean shouldRetry = true;
      try {
        response = delegate.send(request, responseBodyHandler);
        if (!isRetryable(response.statusCode())) {
          return response;
        }
      } catch (IOException e) {
        lastException = e;
        shouldRetry = isRecoverableException(e);
      }

      if (shouldRetry && retryPolicy.allowRetry(attempt)) {
        try {
          clock.sleep(retryPolicy.sleepTime(attempt));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw interrupted;
        }
      } else {
        break;
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
      throw new IOException(
          String.format(
              "Failed HTTP request after %s attempts with elapsed time %s ms",
              retryPolicy.maxAttempts(), elapsedMs));
    }
  }

  private static boolean isRetryable(int statusCode) {
    return RECOVERABLE_STATUS_CODES.contains(statusCode) || (statusCode >= 500 && statusCode < 600);
  }

  private boolean isRecoverableException(Throwable e) {
    // Check the entire exception cause chain for recoverable exceptions
    return Throwables.getCausalChain(e).stream()
        .anyMatch(
            cause ->
                RECOVERABLE_EXCEPTIONS.stream()
                    .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
  }
}
