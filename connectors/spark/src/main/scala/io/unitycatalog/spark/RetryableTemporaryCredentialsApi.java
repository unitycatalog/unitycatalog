package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.base.Throwables;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.utils.Clock;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class RetryableTemporaryCredentialsApi {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<Integer> RECOVERABLE_HTTP_CODES = Set.of(
      429,  // Too Many Requests
      503   // Service Unavailable
  );

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

  private final TemporaryCredentialsApi delegate;
  private final Clock clock;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;

  @FunctionalInterface
  private interface ApiCallSupplier {
    TemporaryCredentials get() throws ApiException;
  }

  public RetryableTemporaryCredentialsApi(TemporaryCredentialsApi delegate, Configuration conf) {
    this(delegate, conf, Clock.systemClock());
  }

  public RetryableTemporaryCredentialsApi(URI uri, String token, Configuration conf) {
    this(new TemporaryCredentialsApi(ApiClientFactory.createApiClient(uri, token)), conf);
  }

  public RetryableTemporaryCredentialsApi(
      TemporaryCredentialsApi delegate, Configuration conf, Clock clock) {
    this.delegate = delegate;
    this.clock = clock;
    this.maxAttempts = conf.getInt(
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY,
        UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT
    );
    this.initialDelayMs = conf.getLong(
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY,
        UCHadoopConf.RETRY_INITIAL_DELAY_DEFAULT
    );
    this.multiplier = conf.getDouble(
        UCHadoopConf.RETRY_MULTIPLIER_KEY,
        UCHadoopConf.RETRY_MULTIPLIER_DEFAULT
    );

    Preconditions.checkArgument(this.maxAttempts >= 1,
        String.format("Retry max attempts must be at least 1, got: %d (%s)",
            this.maxAttempts, UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY));
    Preconditions.checkArgument(this.initialDelayMs > 0,
        String.format("Retry initial delay must be positive, got: %d ms (%s)",
            this.initialDelayMs, UCHadoopConf.RETRY_INITIAL_DELAY_KEY));
    Preconditions.checkArgument(this.multiplier > 0,
        String.format("Retry multiplier must be positive, got: %.2f (%s)",
            this.multiplier, UCHadoopConf.RETRY_MULTIPLIER_KEY));
  }

  public TemporaryCredentials generateTemporaryPathCredentials(
      GenerateTemporaryPathCredential request) throws ApiException {
    return callWithRetry(() -> delegate.generateTemporaryPathCredentials(request));
  }

  public TemporaryCredentials generateTemporaryTableCredentials(
      GenerateTemporaryTableCredential request) throws ApiException {
    return callWithRetry(() -> delegate.generateTemporaryTableCredentials(request));
  }

  private TemporaryCredentials callWithRetry(ApiCallSupplier apiCall) throws ApiException {
    Exception lastException = null;
    Instant startTime = clock.now();

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return apiCall.get();
      } catch (Exception e) {
        lastException = e;

        if (!isRecoverable(e) || attempt == maxAttempts) {
          break;
        }

        long baseDelay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
        double jitter = (Math.random() - 0.5) * 2 * UCHadoopConf.RETRY_JITTER_FACTOR;
        long delay = (long) (baseDelay * (1 + jitter));

        try {
          clock.sleep(Duration.ofMillis(delay));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Retry interrupted", interrupted);
        }
      }
    }

    long elapsedMs = Duration.between(startTime, clock.now()).toMillis();
    if (lastException instanceof ApiException) {
      throw (ApiException) lastException;
    } else {
      throw new RuntimeException(
          "Failed to obtain temporary credentials after " + maxAttempts + " attempts" +
              " (elapsed time: " + elapsedMs + "ms)",
          lastException
      );
    }
  }

  private boolean isRecoverable(Throwable e) {
    if (e instanceof ApiException) {
      ApiException apiEx = (ApiException) e;
      int code = apiEx.getCode();
      if (RECOVERABLE_HTTP_CODES.contains(code)) {
        return true;
      }

      String errorCode = extractUcErrorCode(apiEx.getResponseBody());
      if (errorCode != null && RECOVERABLE_ERROR_CODES.contains(errorCode)) {
        return true;
      }
      return false;
    }

    // Check the entire exception cause chain for network exceptions
    return Throwables.getCausalChain(e).stream()
        .anyMatch(cause -> RECOVERABLE_NETWORK_EXCEPTIONS.stream()
            .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
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
