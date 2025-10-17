package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.utils.Clock;
import java.time.Duration;
import org.apache.hadoop.conf.Configuration;

public class RetryableTemporaryCredentialsApi {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
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

  RetryableTemporaryCredentialsApi(
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

    if (lastException instanceof ApiException) {
      throw (ApiException) lastException;
    } else {
      throw new RuntimeException(
          "Failed to obtain temporary credentials after " + maxAttempts + " attempts",
          lastException
      );
    }
  }

  private boolean isRecoverable(Throwable e) {
    if (e instanceof ApiException) {
      ApiException apiEx = (ApiException) e;
      int code = apiEx.getCode();
      if (code == 429 || code == 503) {
        return true;
      }
      
      String errorCode = extractUcErrorCode(apiEx.getResponseBody());
      if (errorCode != null) {
        return errorCode.equals("TEMPORARILY_UNAVAILABLE")
            || errorCode.equals("WORKSPACE_TEMPORARILY_UNAVAILABLE")
            || errorCode.equals("SERVICE_UNDER_MAINTENANCE");
      }
      return false;
    }
    
    return e instanceof java.net.SocketTimeoutException
        || e instanceof java.net.SocketException
        || e instanceof java.net.UnknownHostException;
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
