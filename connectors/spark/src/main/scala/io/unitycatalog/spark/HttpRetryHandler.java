package io.unitycatalog.spark;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Interface for determining whether an HTTP request should be retried.
 *
 * <p>This interface allows users to implement custom retry logic for both
 * exceptions and HTTP responses. Implementations should be stateless and thread-safe.
 */
public interface HttpRetryHandler {

  <T> HttpResponse<T> call(
      HttpClient delegate,
      HttpRequest request,
      HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException;
}

