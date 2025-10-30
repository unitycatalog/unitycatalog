package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sparkproject.guava.base.Throwables;
import org.sparkproject.guava.collect.ImmutableSet;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Default implementation of {@link HttpRetryHandler} that retries on common
 * recoverable HTTP errors and network exceptions.
 */
public class DefaultHttpRetryHandler implements HttpRetryHandler {
  private static final ImmutableSet<Integer> RECOVERABLE_STATUS_CODES =
      ImmutableSet.of(429, 503);

  private static final ImmutableSet<Class<? extends Throwable>> RECOVERABLE_EXCEPTIONS =
      ImmutableSet.of(
          java.net.SocketTimeoutException.class,
          java.net.SocketException.class,
          java.net.UnknownHostException.class);

  private static final ImmutableSet<String> RECOVERABLE_ERROR_CODES =
      ImmutableSet.of(
          "TEMPORARILY_UNAVAILABLE",
          "WORKSPACE_TEMPORARILY_UNAVAILABLE",
          "SERVICE_UNDER_MAINTENANCE");

  @Override
  public boolean shouldRetryOnException(HttpRequest request, Exception exception) {
    return Throwables.getCausalChain(exception).stream()
        .anyMatch(cause -> RECOVERABLE_EXCEPTIONS.stream()
            .anyMatch(exceptionClass -> exceptionClass.isInstance(cause)));
  }

  @Override
  public boolean shouldRetryOnResponse(
      HttpRequest request,
      HttpResponse<?> response,
      String responseBody) {
    int status = response.statusCode();
    
    if (RECOVERABLE_STATUS_CODES.contains(status)) {
      return true;
    }

    if (status >= 500 && status < 600) {
      String errorCode = extractErrorCode(responseBody);
      return errorCode != null && RECOVERABLE_ERROR_CODES.contains(errorCode);
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

