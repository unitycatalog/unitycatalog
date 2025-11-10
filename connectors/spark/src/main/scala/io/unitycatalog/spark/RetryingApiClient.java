package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.spark.utils.Clock;
import java.net.http.HttpClient;

public class RetryingApiClient extends ApiClient {

  private final HttpRetryHandler retryHandler;

  public RetryingApiClient(ApiClientConf apiClientConf, Clock clock) {
    ApiClientConf effectiveConf = apiClientConf != null ? apiClientConf : new ApiClientConf();
    Clock effectiveClock = clock != null ? clock : Clock.systemClock();
    this.retryHandler = new HttpRetryHandler(effectiveConf, effectiveClock);
  }

  @Override
  public HttpClient getHttpClient() {
    HttpClient baseClient = super.getHttpClient();
    return new RetryingHttpClient(baseClient, retryHandler);
  }
}

