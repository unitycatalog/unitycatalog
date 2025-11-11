package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.spark.utils.Clock;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class HttpRetryHandlerTest {
  private Clock.ManualClock clock;

  @BeforeEach
  public void setUp() {
    clock = (Clock.ManualClock) Clock.manualClock(Instant.now());
  }

  @AfterEach
  public void tearDown() {
    clock = null;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetrySucceedsAfterTwoFailures() throws IOException, InterruptedException {
    ApiClientConf conf =
        new ApiClientConf()
            .setRequestMaxAttempts(5)
            .setRequestInitialDelayMs(100L)
            .setRequestMultiplier(2.0)
            .setRequestJitterFactor(0.0); // Disable jitter

    HttpClient mockClient = mock(HttpClient.class);
    HttpRequest mockRequest =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/api/test")).build();
    HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

    HttpResponse<String> response503 = createMockResponse(503, "Service Unavailable");
    HttpResponse<String> response429 = createMockResponse(429, "Too Many Requests");
    HttpResponse<String> response200 = createMockResponse(200, "Success");

    // Configure mock to fail twice, then succeed
    when(mockClient.send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
        .thenReturn(response503)
        .thenReturn(response429)
        .thenReturn(response200);

    Instant start = clock.now();
    HttpRetryHandler handler = new HttpRetryHandler(conf, clock);
    HttpResponse<String> result = handler.call(mockClient, mockRequest, bodyHandler);

    verify(mockClient, times(3))
        .send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());

    assertThat(result.statusCode()).isEqualTo(200);
    assertThat(result.body()).isEqualTo("Success");

    // Verify clock advanced for retries (2 retries)
    // First retry delay: 100ms * 2^0 = 100ms
    // Second retry delay: 100ms * 2^1 = 200ms
    // Total: 300ms
    Instant expectedTime = start.plusMillis(300);
    assertThat(clock.now()).isAfterOrEqualTo(expectedTime);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetryServerErrorAppliesJitterWithinBounds()
      throws IOException, InterruptedException {
    double jitterFactor = 0.5;
    ApiClientConf conf =
        new ApiClientConf()
            .setRequestMaxAttempts(2)
            .setRequestInitialDelayMs(100L)
            .setRequestMultiplier(1.0)
            .setRequestJitterFactor(jitterFactor);

    HttpClient mockClient = mock(HttpClient.class);
    HttpRequest mockRequest =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/api/server-error")).build();
    HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

    HttpResponse<String> response503 = createMockResponse(503, "Service Unavailable");
    HttpResponse<String> response200 = createMockResponse(200, "Recovered");

    when(mockClient.send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
        .thenReturn(response503)
        .thenReturn(response200);

    Instant start = clock.now();
    HttpRetryHandler handler = new HttpRetryHandler(conf, clock);
    HttpResponse<String> result = handler.call(mockClient, mockRequest, bodyHandler);

    verify(mockClient, times(2))
        .send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());

    assertThat(result.statusCode()).isEqualTo(200);
    assertThat(result.body()).isEqualTo("Recovered");

    // Verify the elapsed time is within the jitter-adjusted bounds of the base delay.
    // Calculated as: baseDelay * (1 ± jitterFactor). In this case, the base delay is 100ms and
    // the jitter factor is 0.5 so the range is [50ms, 150ms].
    long elapsedMs = Duration.between(start, clock.now()).toMillis();
    long baseDelay = conf.getRequestInitialDelayMs();
    long minDelay = (long) Math.floor(baseDelay * (1 - jitterFactor));
    long maxDelay = (long) Math.ceil(baseDelay * (1 + jitterFactor));
    assertThat(elapsedMs)
        .as("retry delay should stay within jitter-adjusted bounds")
        .isBetween(minDelay, maxDelay);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetriesRecoverableException() throws IOException, InterruptedException {
    ApiClientConf conf =
        new ApiClientConf()
            .setRequestMaxAttempts(3)
            .setRequestInitialDelayMs(50L)
            .setRequestMultiplier(2.0)
            .setRequestJitterFactor(0.0); // Disable jitter

    HttpClient mockClient = mock(HttpClient.class);
    HttpRequest mockRequest =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/api/simple")).build();
    HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

    HttpResponse<String> response503 = createMockResponse(503, "Service Unavailable");
    HttpResponse<String> response200 = createMockResponse(200, "Success");

    when(mockClient.send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
        .thenThrow(new java.net.SocketTimeoutException("Transient error"))
        .thenReturn(response503)
        .thenReturn(response200);

    Instant start = clock.now();
    HttpRetryHandler handler = new HttpRetryHandler(conf, clock);
    HttpResponse<String> result = handler.call(mockClient, mockRequest, bodyHandler);

    verify(mockClient, times(3))
        .send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
    assertThat(result.statusCode()).isEqualTo(200);
    // Retry delays: 50ms (after exception) + 100ms (after 503) = 150ms total.
    assertThat(clock.now()).isEqualTo(start.plus(Duration.ofMillis(150)));
  }

  @SuppressWarnings("unchecked")
  private HttpResponse<String> createMockResponse(int statusCode, String body) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);
    when(response.body()).thenReturn(body);
    return response;
  }
}
