package io.unitycatalog.spark;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;
import org.apache.hadoop.conf.Configuration;

public class RetryableTemporaryCredentialsApi {
  private final TemporaryCredentialsApi delegate;
  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;

  @FunctionalInterface
  private interface ApiCallSupplier {
    TemporaryCredentials get() throws ApiException;
  }

  public RetryableTemporaryCredentialsApi(TemporaryCredentialsApi delegate, Configuration conf) {
    this.delegate = delegate;
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
          Thread.sleep(delay);
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
      int code = ((ApiException) e).getCode();
      // Retry on rate limit (429), service unavailable (503), and 5xx server errors
      return code == 429 || code == 503 || (code >= 500 && code < 600);
    }
    // Network-level transient failures
    return e instanceof java.net.SocketTimeoutException  // Timeout
        || e instanceof java.net.SocketException           // Connection issues
        || e instanceof java.net.UnknownHostException;     // DNS resolution issues
  }
}

