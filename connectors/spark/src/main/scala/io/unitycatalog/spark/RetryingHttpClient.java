package io.unitycatalog.spark;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.spark.utils.Clock;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

public class RetryingHttpClient extends HttpClient {
  private final HttpClient delegate;
  private final HttpRetryHandler retryHandler;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;
  private final double jitterFactor;
  private final Clock clock;
  private final Logger logger;

  public RetryingHttpClient(
      HttpClient delegate,
      HttpRetryHandler retryHandler,
      int maxAttempts,
      long initialDelayMs,
      double multiplier,
      double jitterFactor,
      Clock clock,
      Logger logger) {
    this.delegate = delegate;
    this.retryHandler = retryHandler;
    this.maxAttempts = maxAttempts;
    this.initialDelayMs = initialDelayMs;
    this.multiplier = multiplier;
    this.jitterFactor = jitterFactor;
    this.clock = clock;
    this.logger = logger;
  }

  @Override
  public Optional<CookieHandler> cookieHandler() {
    return delegate.cookieHandler();
  }

  @Override
  public Optional<Duration> connectTimeout() {
    return delegate.connectTimeout();
  }

  @Override
  public Redirect followRedirects() {
    return delegate.followRedirects();
  }

  @Override
  public Optional<ProxySelector> proxy() {
    return delegate.proxy();
  }

  @Override
  public SSLContext sslContext() {
    return delegate.sslContext();
  }

  @Override
  public SSLParameters sslParameters() {
    return delegate.sslParameters();
  }

  @Override
  public Optional<Authenticator> authenticator() {
    return delegate.authenticator();
  }

  @Override
  public Version version() {
    return delegate.version();
  }

  @Override
  public Optional<Executor> executor() {
    return delegate.executor();
  }

  @Override
  public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
      throws IOException, InterruptedException {
    Exception lastException = null;
    var startTime = clock.now();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        HttpResponse<T> response = delegate.send(request, handler);
        String responseBody = readResponseBodyAsString(response);

        // Check if the response should be retried (and we have retries left)
        if (attempt < maxAttempts
            && retryHandler.shouldRetryOnResponse(request, response, responseBody)) {
          lastException = new IOException(
              "Recoverable response: status=" + response.statusCode() + ", body=" + responseBody);
          closeResponse(response);
          long delay = calculateDelay(attempt);
          long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
          logRetry(attempt, "status=" + response.statusCode(), delay, elapsedMs);
          clock.sleep(Duration.ofMillis(delay));
          continue;
        }

        return response;
      } catch (Exception e) {
        lastException = e;

        // Check if we've exceeded max attempts or if the exception should be retried
        if (attempt >= maxAttempts || !retryHandler.shouldRetryOnException(request, e)) {
          throw e;
        }

        long delay = calculateDelay(attempt);
        long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
        logRetry(attempt, e.getClass().getSimpleName() + ": " + e.getMessage(), delay, elapsedMs);
        clock.sleep(Duration.ofMillis(delay));
      }
    }

    // If we don't have any attempts left, we should throw the last exception upwards if available
    long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
    if (lastException == null) {
      throw new RuntimeException(
          "Retry loop ended unexpectedly after " + elapsedMs + "ms with no exception");
    }
    if (lastException instanceof IOException) {
      throw (IOException) lastException;
    }
    if (lastException instanceof InterruptedException) {
      throw (InterruptedException) lastException;
    }
    throw new RuntimeException(
        "Retry loop ended unexpectedly after " + elapsedMs + "ms", lastException);
  }

  @Override
  public <T> CompletableFuture<HttpResponse<T>> sendAsync(
      HttpRequest request,
      HttpResponse.BodyHandler<T> handler) {
    // Delegate without retry for async calls
    // Future enhancement: implement async retry logic
    return delegate.sendAsync(request, handler);
  }

  @Override
  public <T> CompletableFuture<HttpResponse<T>> sendAsync(
      HttpRequest request,
      HttpResponse.BodyHandler<T> handler,
      HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
    // Delegate without retry for async calls
    return delegate.sendAsync(request, handler, pushPromiseHandler);
  }

  private long calculateDelay(int attempt) {
    long baseDelay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
    double jitter = (Math.random() - 0.5) * 2 * jitterFactor;
    return (long) (baseDelay * (1 + jitter));
  }

  private void logRetry(int attempt, Object reason, long delay, long elapsedMs) {
    logger.log(
        Level.WARNING,
        "HTTP request failed (attempt {0}, elapsed: {1}ms): {2}. Retrying after {3}ms",
        new Object[]{attempt, elapsedMs, reason, delay});
  }

  private String readResponseBodyAsString(HttpResponse<?> response) {
    Object body = response.body();
    if (body == null) {
      return "";
    }
    if (body instanceof InputStream) {
      try (InputStream is = (InputStream) body) {
        return new String(is.readAllBytes());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    if (body instanceof String) {
      return (String) body;
    }
    return body.toString();
  }

  private void closeResponse(HttpResponse<?> response) {
    Object body = response.body();
    if (body instanceof AutoCloseable) {
      try {
        ((AutoCloseable) body).close();
      } catch (Exception ignored) {
        // Ignore close failures
      }
    }
  }
}

