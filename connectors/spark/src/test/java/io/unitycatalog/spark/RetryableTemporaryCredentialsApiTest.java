package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.utils.Clock;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class RetryableTemporaryCredentialsApiTest {

  private Configuration conf;
  private TemporaryCredentialsApi delegate;
  private RetryableTemporaryCredentialsApi retryableApi;

  private void initRetryableApi() {
    Clock clock = Clock.manualClock(Instant.now());
    retryableApi = new RetryableTemporaryCredentialsApi(delegate, conf, clock);
  }

  @BeforeEach
  public void setUp() {
    conf = new Configuration();
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT);
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, UCHadoopConf.RETRY_INITIAL_DELAY_DEFAULT);
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, UCHadoopConf.RETRY_MULTIPLIER_DEFAULT);

    delegate = Mockito.mock(TemporaryCredentialsApi.class);
    initRetryableApi();
  }

  @Test
  public void testSuccessNoRetryNeededForTable() throws Exception {
    TemporaryCredentials expected = new TemporaryCredentials();
    when(delegate.generateTemporaryTableCredentials(any(GenerateTemporaryTableCredential.class)))
        .thenReturn(expected);

    TemporaryCredentials actual =
        retryableApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential().tableId("table").operation(null));

    assertThat(actual).isSameAs(expected);
    verify(delegate, times(1)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testSuccessNoRetryNeededForPath() throws Exception {
    TemporaryCredentials expected = new TemporaryCredentials();
    when(delegate.generateTemporaryPathCredentials(any(GenerateTemporaryPathCredential.class)))
        .thenReturn(expected);

    TemporaryCredentials actual =
        retryableApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential().url("/test").operation(null));

    assertThat(actual).isSameAs(expected);
    verify(delegate, times(1)).generateTemporaryPathCredentials(any());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recoverableErrorProvider")
  public void testRecoverableErrorEventuallySucceedsForTable(
      String description, Exception firstError, Exception secondError) throws Exception {
    TemporaryCredentials expected = new TemporaryCredentials();
    when(delegate.generateTemporaryTableCredentials(any(GenerateTemporaryTableCredential.class)))
        .thenThrow(firstError)
        .thenThrow(secondError)
        .thenReturn(expected);

    TemporaryCredentials actual =
        retryableApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential().tableId("table").operation(null));

    assertThat(actual).isSameAs(expected);
    verify(delegate, times(3)).generateTemporaryTableCredentials(any());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("recoverableErrorProvider")
  public void testRecoverableErrorEventuallySucceedsForPath(
      String description, Exception firstError, Exception secondError) throws Exception {
    TemporaryCredentials expected = new TemporaryCredentials();
    when(delegate.generateTemporaryPathCredentials(any(GenerateTemporaryPathCredential.class)))
        .thenThrow(firstError)
        .thenThrow(secondError)
        .thenReturn(expected);

    TemporaryCredentials actual =
        retryableApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential().url("/test").operation(null));

    assertThat(actual).isSameAs(expected);
    verify(delegate, times(3)).generateTemporaryPathCredentials(any());
  }

  private static Stream<Arguments> recoverableErrorProvider() {
    return Stream.of(
        // Mix HTTP status codes with UC error codes
        Arguments.of("HTTP 429 → HTTP 503", apiException(429), apiException(503)),
        Arguments.of(
            "HTTP 503 → UC TEMPORARILY_UNAVAILABLE",
            apiException(503),
            apiException(500, "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\"}")),

        // Mix UC error codes with network exceptions
        Arguments.of(
            "UC WORKSPACE_TEMPORARILY_UNAVAILABLE → Network SocketTimeout",
            apiException(500, "{\"error_code\":\"WORKSPACE_TEMPORARILY_UNAVAILABLE\"}"),
            new RuntimeException(new SocketTimeoutException("timeout"))),
        Arguments.of(
            "UC SERVICE_UNDER_MAINTENANCE → Network SocketException",
            apiException(500, "{\"error_code\":\"SERVICE_UNDER_MAINTENANCE\"}"),
            new RuntimeException(new SocketException("connection reset"))),

        // Mix different network exceptions
        Arguments.of(
            "Network SocketTimeout → Network UnknownHost",
            new RuntimeException(new SocketTimeoutException("timeout")),
            new RuntimeException(new UnknownHostException("unknown host"))),
        Arguments.of(
            "Network SocketException → HTTP 429",
            new RuntimeException(new SocketException("connection reset")),
            apiException(429)));
  }

  @Test
  public void testNonRecoverableHttpStopsImmediately() throws Exception {
    when(delegate.generateTemporaryPathCredentials(any(GenerateTemporaryPathCredential.class)))
        .thenThrow(apiException(400));

    assertThatThrownBy(
            () ->
                retryableApi.generateTemporaryPathCredentials(
                    new GenerateTemporaryPathCredential().url("/test").operation(null)))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", 400);

    verify(delegate, times(1)).generateTemporaryPathCredentials(any());
  }

  @Test
  public void testMaxAttemptsExhausted() throws Exception {
    when(delegate.generateTemporaryTableCredentials(any(GenerateTemporaryTableCredential.class)))
        .thenThrow(apiException(503));

    assertThatThrownBy(
            () ->
                retryableApi.generateTemporaryTableCredentials(
                    new GenerateTemporaryTableCredential().tableId("table").operation(null)))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", 503);

    verify(delegate, times(UCHadoopConf.RETRY_MAX_ATTEMPTS_DEFAULT))
        .generateTemporaryTableCredentials(any());
  }

  @Test
  public void testConfigurationOverridesAffectBehaviour() throws Exception {
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, 5);
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, 1000L);
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, 2.0);

    initRetryableApi();

    TemporaryCredentials expected = new TemporaryCredentials();
    when(delegate.generateTemporaryPathCredentials(any(GenerateTemporaryPathCredential.class)))
        .thenThrow(apiException(503))
        .thenThrow(apiException(503))
        .thenThrow(apiException(503))
        .thenThrow(apiException(503))
        .thenReturn(expected);

    TemporaryCredentials actual =
        retryableApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential().url("/tmp").operation(null));

    assertThat(actual).isSameAs(expected);
    verify(delegate, times(5)).generateTemporaryPathCredentials(any());
  }

  @Test
  public void testInvalidJsonFallbacksToStatus() throws Exception {
    when(delegate.generateTemporaryPathCredentials(any(GenerateTemporaryPathCredential.class)))
        .thenThrow(apiException(503, "not-json"))
        .thenThrow(apiException(404))
        .thenReturn(new TemporaryCredentials());

    assertThatThrownBy(
            () ->
                retryableApi.generateTemporaryPathCredentials(
                    new GenerateTemporaryPathCredential().url("/tmp").operation(null)))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", 404);

    verify(delegate, times(2)).generateTemporaryPathCredentials(any());
  }

  @Test
  public void testOneMaxAttemptMeansNoRetry() throws Exception {
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, 1);
    initRetryableApi();

    when(delegate.generateTemporaryTableCredentials(any(GenerateTemporaryTableCredential.class)))
        .thenThrow(apiException(503));

    assertThatThrownBy(
            () ->
                retryableApi.generateTemporaryTableCredentials(
                    new GenerateTemporaryTableCredential().tableId("table").operation(null)))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", 503);

    verify(delegate, times(1)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testInvalidMaxAttemptsThrowsException() {
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, 0);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry max attempts must be at least 1")
        .hasMessageContaining("got: 0");
  }

  @Test
  public void testNegativeMaxAttemptsThrowsException() {
    conf.setInt(UCHadoopConf.RETRY_MAX_ATTEMPTS_KEY, -5);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry max attempts must be at least 1")
        .hasMessageContaining("got: -5");
  }

  @Test
  public void testNegativeInitialDelayThrowsException() {
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, -1000L);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry initial delay must be positive")
        .hasMessageContaining("got: -1000");
  }

  @Test
  public void testZeroMultiplierThrowsException() {
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, 0.0);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry multiplier must be positive")
        .hasMessageContaining("got: 0.0");
  }

  @Test
  public void testNegativeMultiplierThrowsException() {
    conf.setDouble(UCHadoopConf.RETRY_MULTIPLIER_KEY, -1.5);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry multiplier must be positive")
        .hasMessageContaining("got: -1.5");
  }

  @Test
  public void testZeroInitialDelayThrowsException() {
    conf.setLong(UCHadoopConf.RETRY_INITIAL_DELAY_KEY, 0L);

    assertThatThrownBy(() -> new RetryableTemporaryCredentialsApi(delegate, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Retry initial delay must be positive")
        .hasMessageContaining("got: 0");
  }

  private static ApiException apiException(int status) {
    return new ApiException(status, "status" + status);
  }

  private static ApiException apiException(int status, String body) {
    return new ApiException("error", status, null, body);
  }
}
