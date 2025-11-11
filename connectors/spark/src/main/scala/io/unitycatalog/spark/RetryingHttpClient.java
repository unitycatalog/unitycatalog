package io.unitycatalog.spark;

import java.io.IOException;
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
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/**
 * Wraps an {@link HttpClient} to add retries.
 *
 * <p>The generated {@link io.unitycatalog.client.ApiClient} is recreated from OpenAPI specs, so we
 * keep retry state outside of it. Handling retries here means every generated endpoint class that
 * relies on the shared {@link io.unitycatalog.client.ApiClient} automatically gets the same
 * behaviour. {@link RetryingApiClient} uses this adapter to run retries through
 * {@link HttpRetryHandler} by overriding {@link #send(HttpRequest, HttpResponse.BodyHandler)} to
 * call {@link HttpRetryHandler#call}, which implements exponential backoff while using the
 * underlying {@link HttpClient#send(HttpRequest, HttpResponse.BodyHandler)} to send the request.
 * </p>
 */
public class RetryingHttpClient extends HttpClient {
  private final HttpClient delegate;
  private final HttpRetryHandler retryHandler;

  public RetryingHttpClient(HttpClient delegate, HttpRetryHandler retryHandler) {
    this.delegate = delegate;
    this.retryHandler = retryHandler;
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
    return retryHandler.call(delegate, request, handler);
  }

  @Override
  public <T> CompletableFuture<HttpResponse<T>> sendAsync(
      HttpRequest request,
      HttpResponse.BodyHandler<T> handler) {
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
}

