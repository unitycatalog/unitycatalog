package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Default implementation of {@link HttpRetryHandler} that retries on common
 * recoverable HTTP errors and network exceptions.
 */
public class DefaultHttpRetryHandler implements HttpRetryHandler {
  private final int maxAttempts;

  public DefaultHttpRetryHandler(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  @Override
  public boolean shouldRetryOnException(HttpRequest request, Exception exception, int attemptCount) {
    if (attemptCount >= maxAttempts) {
      return false;
    }

    // Check if exception or its causes are recoverable network exceptions
    Throwable current = exception;
    while (current != null) {
      if (current instanceof java.net.SocketTimeoutException
          || current instanceof java.net.SocketException
          || current instanceof java.net.UnknownHostException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Override
  public boolean shouldRetryOnResponse(
      HttpRequest request,
      HttpResponse<?> response,
      String responseBody,
      int attemptCount) {
    if (attemptCount >= maxAttempts) {
      return false;
    }

    int status = response.statusCode();

    // Retry on specific recoverable HTTP codes
    if (status == 429 || status == 503) {
      return true;
    }

    // For 5xx errors, check for Unity Catalog specific error codes
    if (status >= 500 && status < 600) {
      String errorCode = extractErrorCode(responseBody);
      if (errorCode != null) {
        if (errorCode.equals("TEMPORARILY_UNAVAILABLE")
            || errorCode.equals("WORKSPACE_TEMPORARILY_UNAVAILABLE")
            || errorCode.equals("SERVICE_UNDER_MAINTENANCE")) {
          return true;
        }
      }
    }

    return false;
  }

  private String extractErrorCode(String body) {
    if (body == null || body.isEmpty()) {
      return null;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(body);
      JsonNode codeNode = node.get("error_code");
      return codeNode != null ? codeNode.asText() : null;
    } catch (Exception e) {
      return null;
    }
  }
}

