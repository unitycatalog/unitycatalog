package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.spark.utils.Clock;
import org.sparkproject.guava.base.Throwables;

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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

public class RetryingHttpClient extends HttpClient {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<Integer> RECOVERABLE_HTTP_CODES = Set.of(429, 503);

  private static final Set<String> RECOVERABLE_ERROR_CODES = Set.of(
      "TEMPORARILY_UNAVAILABLE",
      "WORKSPACE_TEMPORARILY_UNAVAILABLE",
      "SERVICE_UNDER_MAINTENANCE"
  );

  private static final Set<Class<? extends Throwable>> RECOVERABLE_NETWORK_EXCEPTIONS = Set.of(
      java.net.SocketTimeoutException.class,
      java.net.SocketException.class,
      java.net.UnknownHostException.class
  );

  private final HttpClient delegate;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;
  private final double jitterFactor;
  private final Clock clock;
  private final Logger logger;

  public RetryingHttpClient(
      HttpClient delegate,
      int maxAttempts,
      long initialDelayMs,
      double multiplier,
      double jitterFactor,
      Clock clock,
      Logger logger) {
    this.delegate = delegate;
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
        int status = response.statusCode();

        // For retryable status codes, we need to read the body to check UC error codes
        if (isRecoverableStatus(status)) {
          String responseBody = readResponseBodyAsString(response);
          if (isRecoverable(status, responseBody)) {
            lastException = new IOException(
                "Recoverable response: status=" + status + ", body=" + responseBody);
            if (attempt == maxAttempts) {
              long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
              throw new RuntimeException(
                  "HTTP request failed after " + maxAttempts + " attempts (elapsed: "
                      + elapsedMs + "ms)",
                  lastException);
            }
            closeResponse(response);
            long delay = calculateDelay(attempt);
            logRetry(attempt, "status=" + status, delay);
            clock.sleep(Duration.ofMillis(delay));
            continue;
          }
        }

        // Non-retryable: Return as-is (API layer handles ApiException)
        return response;
      } catch (Exception e) {
        lastException = e;
        if (!isRecoverableException(e)) {
          throw e;
        }
        if (attempt == maxAttempts) {
          long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
          throw new RuntimeException(
              "HTTP request failed after " + maxAttempts + " attempts (elapsed: "
                  + elapsedMs + "ms)",
              lastException);
        }
        long delay = calculateDelay(attempt);
        logRetry(attempt, e.getClass().getSimpleName() + ": " + e.getMessage(), delay);
        clock.sleep(Duration.ofMillis(delay));
      }
    }

    // Should never reach here, but just in case
    long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
    throw new RuntimeException(
        "HTTP request failed after " + maxAttempts + " attempts (elapsed: " + elapsedMs + "ms)",
        lastException);
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

  private boolean isRecoverableStatus(int status) {
    return RECOVERABLE_HTTP_CODES.contains(status) || (status >= 500 && status < 600);
  }

  private boolean isRecoverable(int status, String body) {
    if (RECOVERABLE_HTTP_CODES.contains(status)) {
      return true;
    }
    if (status >= 500 && status < 600) {
      // Check for Unity Catalog specific error codes in 5xx responses
      String errorCode = extractUcErrorCode(body);
      return errorCode != null && RECOVERABLE_ERROR_CODES.contains(errorCode);
    }
    return false;
  }

  private boolean isRecoverableException(Exception e) {
    return Throwables.getCausalChain(e).stream()
        .anyMatch(cause -> RECOVERABLE_NETWORK_EXCEPTIONS.stream()
            .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
  }

  private long calculateDelay(int attempt) {
    long baseDelay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
    double jitter = (Math.random() - 0.5) * 2 * jitterFactor;
    return (long) (baseDelay * (1 + jitter));
  }

  private void logRetry(int attempt, Object reason, long delay) {
    logger.log(
        Level.WARNING,
        "HTTP request failed (attempt {0}/{1}): {2}. Retrying after {3}ms",
        new Object[]{attempt, maxAttempts, reason, delay});
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

  private static String extractUcErrorCode(String body) {
    if (body == null || body.isEmpty()) {
      return null;
    }
    try {
      JsonNode node = OBJECT_MAPPER.readTree(body);
      JsonNode codeNode = node.get("error_code");
      return codeNode != null ? codeNode.asText() : null;
    } catch (Exception ignore) {
      return null;
    }
  }
}

