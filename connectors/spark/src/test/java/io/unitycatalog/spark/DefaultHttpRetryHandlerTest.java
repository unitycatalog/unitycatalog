package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.spark.utils.Clock;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class DefaultHttpRetryHandlerTest {
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
    Configuration conf = new Configuration();
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, 5);
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, 100L);
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, 2.0);
    conf.setDouble(UCHadoopConf.RETRY_JITTER_FACTOR_KEY, 0.0); // Disable jitter

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
    DefaultHttpRetryHandler handler = new DefaultHttpRetryHandler(conf, clock);
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
  public void testRetryOnRecoverableErrorCodeWithInputStream()
      throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, 3);
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, 10L);
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, 1.0);
    conf.setDouble(UCHadoopConf.RETRY_JITTER_FACTOR_KEY, 0.0);

    HttpClient mockClient = mock(HttpClient.class);
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/api/test")).build();
    HttpResponse.BodyHandler<InputStream> bodyHandler = HttpResponse.BodyHandlers.ofInputStream();

    byte[] retryBody =
        "{\"error_code\":\"SERVICE_UNDER_MAINTENANCE\"}".getBytes(StandardCharsets.UTF_8);
    HttpResponse<InputStream> response503 = mock(HttpResponse.class);
    when(response503.statusCode()).thenReturn(503);
    when(response503.body()).thenAnswer(inv -> new ByteArrayInputStream(retryBody));

    byte[] successBody = "{}".getBytes(StandardCharsets.UTF_8);
    HttpResponse<InputStream> response200 = mock(HttpResponse.class);
    when(response200.statusCode()).thenReturn(200);
    when(response200.body()).thenAnswer(inv -> new ByteArrayInputStream(successBody));

    when(mockClient.send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<InputStream>>any()))
        .thenReturn(response503)
        .thenReturn(response200);

    DefaultHttpRetryHandler handler = new DefaultHttpRetryHandler(conf, clock);
    HttpResponse<InputStream> result = handler.call(mockClient, request, bodyHandler);

    verify(mockClient, times(2))
        .send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<InputStream>>any());
    assertThat(result.statusCode()).isEqualTo(200);
    assertThat(new String(result.body().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{}");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleRetryHandlerRetriesOnce() throws IOException, InterruptedException {
    HttpClient mockClient = mock(HttpClient.class);
    HttpRequest mockRequest =
        HttpRequest.newBuilder().uri(URI.create("http://localhost:8080/api/simple")).build();
    HttpResponse.BodyHandler<String> bodyHandler = HttpResponse.BodyHandlers.ofString();

    HttpResponse<String> response200 = createMockResponse(200, "Success");

    when(mockClient.send(
            any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
        .thenThrow(new IOException("Transient error"))
        .thenReturn(response200);

    Instant start = clock.now();
    SimpleRetryHandler handler = new SimpleRetryHandler(clock, 2, 50L);
    HttpResponse<String> result = handler.call(mockClient, mockRequest, bodyHandler);

    verify(mockClient, times(2))
        .send(any(HttpRequest.class), ArgumentMatchers.<HttpResponse.BodyHandler<String>>any());
    assertThat(result.statusCode()).isEqualTo(200);
    assertThat(clock.now()).isEqualTo(start.plusMillis(50));
  }

  private static class SimpleRetryHandler implements HttpRetryHandler {
    private final Clock clock;
    private final int maxAttempts;
    private final long delayMs;

    SimpleRetryHandler(Clock clock, int maxAttempts, long delayMs) {
      this.clock = clock;
      this.maxAttempts = maxAttempts;
      this.delayMs = delayMs;
    }

    @Override
    public <T> HttpResponse<T> call(
        HttpClient delegate, HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException {
      IOException lastException = null;

      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          return delegate.send(request, responseBodyHandler);
        } catch (IOException e) {
          lastException = e;
          if (attempt == maxAttempts) {
            throw e;
          }
          clock.sleep(Duration.ofMillis(delayMs));
        }
      }

      throw lastException;
    }
  }

  @SuppressWarnings("unchecked")
  private HttpResponse<String> createMockResponse(int statusCode, String body) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(statusCode);
    when(response.body()).thenReturn(body);
    return response;
  }
}
