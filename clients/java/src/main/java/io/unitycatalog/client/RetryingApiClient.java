package io.unitycatalog.client;

import io.unitycatalog.client.retry.RetryPolicy;
import io.unitycatalog.client.utils.Clock;
import java.net.http.HttpClient;

/**
 * Adds retry handling on top of the generated {@link ApiClient}.
 *
 * <p>The base {@link ApiClient} comes from the OpenAPI generator and is overwritten on every regen.
 * To keep that file untouched, this subclass overrides {@link #getHttpClient()} and wraps the
 * generated {@link HttpClient} in a {@link RetryingHttpClient} configured through {@link
 * RetryPolicy} and {@link HttpRetryHandler} (which defines the retry mechanism). Adding retries
 * at the HTTP layer means every generated API surface (for example {@code TemporaryCredentialsApi},
 * {@code TablesApi}, etc.) inherits the same policy without extra wrappers.
 */
public class RetryingApiClient extends ApiClient {

  private final HttpRetryHandler retryHandler;

  public RetryingApiClient(RetryPolicy retryPolicy, Clock clock) {
    Clock effectiveClock = clock != null ? clock : Clock.systemClock();
    this.retryHandler = new HttpRetryHandler(retryPolicy, effectiveClock);
  }

  @Override
  public HttpClient getHttpClient() {
    HttpClient baseClient = super.getHttpClient();
    return new RetryingHttpClient(baseClient, retryHandler);
  }
}
