package io.unitycatalog.client.internal;

import io.unitycatalog.client.retry.RetryPolicy;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
    HttpResponse<T> lastResponse = null;

    for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
      // Reset per attempt so the post-loop branch only sees the final attempt's state.
      lastException = null;
      lastResponse = null;
      boolean shouldRetry = true;
      try {
        lastResponse = delegate.send(request, responseBodyHandler);
        if (!isRetryable(lastResponse.statusCode())) {
          return lastResponse;
        }
      } catch (IOException e) {
        lastException = e;
        shouldRetry = isCausedByRecoverableEx(e);
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

    // Return the last retryable response if available, so the caller sees the original error
    // (e.g., 429) instead of a generic IOException.
    if (lastResponse != null) {
      return lastResponse;
    }
    if (lastException != null) {
      throw lastException;
    }
    // Unreachable when maxAttempts >= 1: every iteration either returns, sets lastResponse, or
    // sets lastException. Only reachable with a misconfigured maxAttempts == 0.
    throw new IllegalStateException(
        "HttpRetryHandler reached unreachable state; maxAttempts must be >= 1, got "
            + retryPolicy.maxAttempts());
  }

  private static boolean isRetryable(int statusCode) {
    return RECOVERABLE_STATUS_CODES.contains(statusCode) || (statusCode >= 500 && statusCode < 600);
  }

  private boolean isRecoverableException(Throwable e) {
    return RECOVERABLE_EXCEPTIONS.stream().anyMatch(exClass -> exClass.isInstance(e));
  }

  private boolean isCausedByRecoverableEx(Throwable e) {
    // Check the entire exception cause chain for recoverable exceptions
    if (isRecoverableException(e)) {
      return true;
    }
    for (Throwable cause = e.getCause(); cause != e && cause != null; cause = cause.getCause()) {
      if (isRecoverableException(cause)) {
        return true;
      }
    }
    return false;
  }
}
