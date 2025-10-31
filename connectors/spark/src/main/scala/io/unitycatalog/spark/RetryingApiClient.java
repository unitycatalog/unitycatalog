package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import org.apache.hadoop.conf.Configuration;
import org.sparkproject.guava.base.Preconditions;

import java.net.URI;
import java.net.http.HttpClient;

public class RetryingApiClient extends ApiClient {

  private final Configuration conf;
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

    this.conf = conf;
    this.clock = clock;

    validateConfiguration();

    if (retryHandler == null) {
      // Use default handler with configuration
      this.retryHandler = new DefaultHttpRetryHandler(conf, clock);
    } else {
      this.retryHandler = retryHandler;
    }
  }

  private void validateConfiguration() {
    int maxAttempts = conf.getInt(
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY,
        UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT);
    long initialDelayMs = conf.getLong(
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY,
        UCHadoopConf.RETRY_INITIAL_DELAY_DEFAULT);
    double multiplier = conf.getDouble(
        UCHadoopConf.RETRY_MULTIPLIER_KEY,
        UCHadoopConf.RETRY_MULTIPLIER_DEFAULT);
    double jitterFactor = conf.getDouble(
        UCHadoopConf.RETRY_JITTER_FACTOR_KEY,
        UCHadoopConf.RETRY_JITTER_FACTOR_DEFAULT);

    Preconditions.checkArgument(
        maxAttempts >= 1,
        "Retry max attempts must be at least 1, got: %d (%s)",
        maxAttempts,
        UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY);

    Preconditions.checkArgument(
        initialDelayMs > 0,
        "Retry initial delay must be positive, got: %d ms (%s)",
        initialDelayMs,
        UCHadoopConf.RETRY_INITIAL_DELAY_KEY);

    Preconditions.checkArgument(
        multiplier > 0,
        "Retry multiplier must be positive, got: %.2f (%s)",
        multiplier,
        UCHadoopConf.RETRY_MULTIPLIER_KEY);

    Preconditions.checkArgument(
        jitterFactor >= 0 && jitterFactor <= 1,
        "Retry jitter factor must be between 0 and 1, got: %.2f (%s)",
        jitterFactor,
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
    return new RetryingHttpClient(baseClient, retryHandler);
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

