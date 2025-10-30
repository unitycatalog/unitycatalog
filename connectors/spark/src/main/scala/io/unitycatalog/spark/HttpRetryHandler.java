package io.unitycatalog.spark;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Interface for determining whether an HTTP request should be retried.
 * 
 * <p>This interface allows users to implement custom retry logic for both
 * exceptions and HTTP responses. Implementations should be stateless and thread-safe.
 */
public interface HttpRetryHandler {
  /**
   * Determines if a request should be retried based on an exception that occurred.
   *
   * @param request The HTTP request that failed
   * @param exception The exception that was thrown
   * @return true if the request should be retried, false otherwise
   */
  boolean shouldRetryOnException(HttpRequest request, Exception exception);

  /**
   * Determines if a request should be retried based on the HTTP response received.
   *
   * @param request The HTTP request that was sent
   * @param response The HTTP response received
   * @param responseBody The response body as a string (may be null if body couldn't be read)
   * @return true if the request should be retried, false otherwise
   */
  boolean shouldRetryOnResponse(
      HttpRequest request,
      HttpResponse<?> response,
      String responseBody);
}

