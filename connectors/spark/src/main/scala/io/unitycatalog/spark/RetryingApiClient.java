package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.logging.Logger;

public class RetryingApiClient extends ApiClient {
  private static final Logger LOGGER = Logger.getLogger(RetryingApiClient.class.getName());

  private final int maxAttempts;
  private final long initialDelayMs;
  private final double multiplier;
  private final double jitterFactor;
  private final Clock clock;
  private HttpRetryHandler retryHandler;

  public RetryingApiClient(Configuration conf) {
    this(conf, Clock.systemClock(), null);
  }

  public RetryingApiClient(Configuration conf, Clock clock) {
    this(conf, clock, null);
  }

  public RetryingApiClient(Configuration conf, Clock clock, HttpRetryHandler retryHandler) {
    super();

    // Allow custom retry handler even if retry is disabled in config
    if (retryHandler == null) {
      Preconditions.checkArgument(
          conf.getBoolean(UCHadoopConf.RETRY_ENABLED_KEY, UCHadoopConf.RETRY_ENABLED_DEFAULT),
          "Retries are disabled; use %s=true", UCHadoopConf.RETRY_ENABLED_KEY);
    }

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

    validateConfiguration();

    if (retryHandler == null) {
      // Use default handler with max attempts from config
      this.retryHandler = new DefaultHttpRetryHandler(this.maxAttempts);
    } else {
      this.retryHandler = retryHandler;
    }
  }

  private void validateConfiguration() {
    Preconditions.checkArgument(
        this.maxAttempts >= 1,
        "Retry max attempts must be at least 1, got: %d (%s)",
        this.maxAttempts,
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY);

    Preconditions.checkArgument(
        this.initialDelayMs > 0,
        "Retry initial delay must be positive, got: %d ms (%s)",
        this.initialDelayMs,
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY);

    Preconditions.checkArgument(
        this.multiplier > 0,
        "Retry multiplier must be positive, got: %.2f (%s)",
        this.multiplier,
        UCHadoopConf.RETRY_MULTIPLIER_KEY);

    Preconditions.checkArgument(
        this.jitterFactor >= 0 && this.jitterFactor <= 1,
        "Retry jitter factor must be between 0 and 1, got: %.2f (%s)",
        this.jitterFactor,
        UCHadoopConf.RETRY_JITTER_FACTOR_KEY);
  }

  /**
   * Sets a custom retry handler for this API client.
   *
   * @param handler The retry handler to use
   * @return This object for method chaining
   */
  public RetryingApiClient setRetryHandler(HttpRetryHandler handler) {
    this.retryHandler = handler;
    return this;
  }

  @Override
  public HttpClient getHttpClient() {
    HttpClient baseClient = super.getHttpClient();
    return new RetryingHttpClient(
        baseClient,
        retryHandler,
        initialDelayMs,
        multiplier,
        jitterFactor,
        clock,
        LOGGER);
  }

  public static RetryingApiClient create(Configuration conf, URI uri, String token) {
    RetryingApiClient client = new RetryingApiClient(conf);
    client.setHost(uri.getHost())
        .setScheme(uri.getScheme())
        .setPort(uri.getPort())
        .setBasePath(uri.getPath());

    if (token != null && !token.isEmpty()) {
      client.setRequestInterceptor(
          request -> request.header("Authorization", "Bearer " + token));
    }

    return client;
  }
}

