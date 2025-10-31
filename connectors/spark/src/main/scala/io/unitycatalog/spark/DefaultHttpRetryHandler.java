package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.logging.Logger;

/**
 * Default implementation of {@link HttpRetryHandler} that retries on common
 * recoverable HTTP errors and network exceptions.
 */
public class DefaultHttpRetryHandler implements HttpRetryHandler {
  private static final Logger LOGGER = Logger.getLogger(DefaultHttpRetryHandler.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<Integer> RECOVERABLE_STATUS_CODES = Set.of(
      429,  // Too Many Requests
      503   // Service Unavailable
  );

  private static final Set<String> RECOVERABLE_ERROR_CODES = Set.of(
      "TEMPORARILY_UNAVAILABLE",
      "WORKSPACE_TEMPORARILY_UNAVAILABLE",
      "SERVICE_UNDER_MAINTENANCE"
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

    Preconditions.checkArgument(this.maxAttempts >= 1,
        String.format("Retry max attempts must be at least 1, got: %d (%s)",
            this.maxAttempts, UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY));
    Preconditions.checkArgument(this.initialDelayMs > 0,
        String.format("Retry initial delay must be positive, got: %d ms (%s)",
            this.initialDelayMs, UCHadoopConf.RETRY_INITIAL_DELAY_KEY));
    Preconditions.checkArgument(this.multiplier > 0,
        String.format("Retry multiplier must be positive, got: %.2f (%s)",
            this.multiplier, UCHadoopConf.RETRY_MULTIPLIER_KEY));
    Preconditions.checkArgument(this.jitterFactor >= 0 && this.jitterFactor < 1,
        String.format("Retry jitter factor must be between 0 and 1 (exclusive), got: %.2f (%s)",
            this.jitterFactor, UCHadoopConf.RETRY_JITTER_FACTOR_KEY));
  }

  public DefaultHttpRetryHandler(Configuration conf) {
    this(conf, Clock.systemClock());
  }

  @Override
  public <T> HttpResponse<T> call(
      HttpClient delegate,
      HttpRequest request,
      HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
    Exception lastException = null;
    Instant startTime = clock.now();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        HttpResponse<T> response = delegate.send(request, responseBodyHandler);
        if (!isRecoverableResponse(response)) {
          return response;
        }

        lastException = new IOException("HTTP " + response.statusCode());

        if (attempt == maxAttempts) {
          break;
        }
      } catch (Exception e) {
        lastException = e;

        if (!isRecoverableException(e) || attempt == maxAttempts) {
          break;
        }
      }

      long baseDelay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
      double jitter = (Math.random() - 0.5) * 2 * jitterFactor;
      long delay = (long) (baseDelay * (1 + jitter));

      try {
        clock.sleep(Duration.ofMillis(delay));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw interrupted;
      }
    }

    long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
    if (lastException instanceof IOException) {
      throw (IOException) lastException;
    } else if (lastException instanceof InterruptedException) {
      throw (InterruptedException) lastException;
    } else {
      throw new RuntimeException(
          "Failed HTTP request after " + maxAttempts + " attempts" +
              " (elapsed time: " + elapsedMs + "ms)",
          lastException
      );
    }
  }

  private boolean isRecoverableResponse(HttpResponse<?> response) {
    int statusCode = response.statusCode();

    if (RECOVERABLE_STATUS_CODES.contains(statusCode)) {
      return true;
    }

    if (statusCode >= 500 && statusCode < 600) {
      String errorCode = extractErrorCode(response);
      return errorCode != null && RECOVERABLE_ERROR_CODES.contains(errorCode);
    }

    return false;
  }

  private boolean isRecoverableException(Throwable e) {
    // Check the entire exception cause chain for recoverable exceptions
    return Throwables.getCausalChain(e).stream()
        .anyMatch(cause -> RECOVERABLE_EXCEPTIONS.stream()
            .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
  }

  private String extractErrorCode(HttpResponse<?> response) {
    Object body = response.body();
    if (!(body instanceof String) || ((String) body).isEmpty()) {
      return null;
    }

    try {
      JsonNode node = OBJECT_MAPPER.readTree((String) body);
      JsonNode codeNode = node.get("error_code");
      return codeNode != null ? codeNode.asText() : null;
    } catch (Exception ignore) {
      return null;
    }
  }
}
