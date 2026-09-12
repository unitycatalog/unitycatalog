package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions;
import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class ADLSStorageCleanupAdapterTest {
  private static final String ACCOUNT = "account.dfs.core.windows.net";
  private static final String LOCATION = "abfss://container@" + ACCOUNT + "/root/table";
  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private DataLakeFileSystemClient client;
  private ADLSStorageCleanupAdapter adapter;

  @BeforeEach
  void setUp() {
    client = mock(DataLakeFileSystemClient.class);
    adapter = adapter(client, LOCATION, TIMEOUT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"abfs", "abfss"})
  void deletesOneExactDirectoryRecursively(String scheme) {
    String location = scheme + "://container@" + ACCOUNT + "/root/table";
    adapter = adapter(client, location, TIMEOUT);

    assertThat(adapter.listBatch(10)).containsExactly(location);
    StorageCleanupAttempt.Result result =
        new StorageCleanupAttempt(10, Duration.ofSeconds(30), TIMEOUT).run(adapter);

    assertThat(result).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);
    assertThat(adapter.listBatch(10)).isEmpty();

    ArgumentCaptor<DataLakePathDeleteOptions> options =
        ArgumentCaptor.forClass(DataLakePathDeleteOptions.class);
    verify(client)
        .deleteDirectoryIfExistsWithResponse(
            eq("root/table"), options.capture(), eq(TIMEOUT), same(Context.NONE));
    assertThat(options.getValue().getIsRecursive()).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  void missingDirectoryIsSuccessAndANewAttemptCanRepeatDelete() {
    Response<Boolean> missing = mock(Response.class);
    when(missing.getValue()).thenReturn(false);
    when(client.deleteDirectoryIfExistsWithResponse(
            anyString(),
            any(DataLakePathDeleteOptions.class),
            any(Duration.class),
            any(Context.class)))
        .thenReturn(missing);

    adapter.deleteBatch(adapter.listBatch(1));
    assertThat(adapter.listBatch(1)).isEmpty();
    ADLSStorageCleanupAdapter retry = adapter(client, LOCATION, TIMEOUT);
    retry.deleteBatch(retry.listBatch(1));

    verify(client, times(2))
        .deleteDirectoryIfExistsWithResponse(
            anyString(),
            any(DataLakePathDeleteOptions.class),
            any(Duration.class),
            any(Context.class));
  }

  @Test
  void propagatesStorageAndTimeoutFailures() {
    DataLakeStorageException storageFailure = mock(DataLakeStorageException.class);
    RuntimeException timeoutFailure = new RuntimeException("timed out");
    when(client.deleteDirectoryIfExistsWithResponse(
            anyString(),
            any(DataLakePathDeleteOptions.class),
            any(Duration.class),
            any(Context.class)))
        .thenThrow(storageFailure)
        .thenThrow(timeoutFailure);

    assertThatThrownBy(() -> adapter.deleteBatch(adapter.listBatch(1))).isSameAs(storageFailure);
    assertThat(adapter.listBatch(1)).containsExactly(LOCATION);
    ADLSStorageCleanupAdapter retry = adapter(client, LOCATION, TIMEOUT);
    assertThatThrownBy(() -> retry.deleteBatch(retry.listBatch(1))).isSameAs(timeoutFailure);
  }

  @Test
  void rejectsInvalidLocations() {
    for (String location :
        List.of(
            "s3://container@account.dfs.core.windows.net/root/table",
            "abfss://account.dfs.core.windows.net/root/table",
            "abfss://container@.dfs.core.windows.net/root/table",
            "abfss://container@account.dfs.core.windows.net/")) {
      assertThatThrownBy(() -> adapter(client, location, TIMEOUT))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("ADLS cleanup requires an abfs or abfss directory location");
    }
  }

  @Test
  void validatesBatchSizeAndExactLocation() {
    for (int limit : List.of(0, -1)) {
      assertThatThrownBy(() -> adapter.listBatch(limit))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup batch size must be positive");
    }
    assertThatCode(() -> adapter.deleteBatch(List.of())).doesNotThrowAnyException();
    for (List<String> locations :
        List.of(
            List.of("abfss://container@" + ACCOUNT + "/root/table-other"),
            List.of(LOCATION + "/child"),
            List.of("abfss://other@" + ACCOUNT + "/root/table"),
            List.of("abfss://container@other.dfs.core.windows.net/root/table"),
            List.of(LOCATION, LOCATION))) {
      assertThatThrownBy(() -> adapter.deleteBatch(locations))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("ADLS cleanup can delete only its exact task directory");
    }
    verify(client, never())
        .deleteDirectoryIfExistsWithResponse(
            anyString(),
            any(DataLakePathDeleteOptions.class),
            any(Duration.class),
            any(Context.class));
  }

  @Test
  void rejectsInvalidTimeouts() {
    for (Duration timeout :
        List.of(
            Duration.ZERO,
            Duration.ofMillis(-1),
            Duration.ofNanos(999_999),
            Duration.ofSeconds((long) Integer.MAX_VALUE + 1))) {
      assertThatThrownBy(() -> adapter(client, LOCATION, timeout))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageStartingWith("ADLS request timeout must be between 1 and ");
    }
    assertThatThrownBy(() -> adapter(client, LOCATION, Duration.ofSeconds(Long.MAX_VALUE)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ADLS request timeout is too large");
  }

  @Test
  void buildsClientFromExactEndpointSasAndPerTryTimeout() {
    DataLakeFileSystemClient builtClient = mock(DataLakeFileSystemClient.class);
    try (MockedConstruction<DataLakeFileSystemClientBuilder> construction =
        Mockito.mockConstruction(
            DataLakeFileSystemClientBuilder.class,
            (builder, context) -> {
              when(builder.endpoint(anyString())).thenReturn(builder);
              when(builder.sasToken(anyString())).thenReturn(builder);
              when(builder.retryOptions(any(RequestRetryOptions.class))).thenReturn(builder);
              when(builder.buildClient()).thenReturn(builtClient);
            })) {
      new ADLSStorageCleanupAdapter("sas-token", NormalizedURL.from(LOCATION), TIMEOUT);

      DataLakeFileSystemClientBuilder builder = construction.constructed().get(0);
      verify(builder).endpoint("https://" + ACCOUNT + "/container");
      verify(builder).sasToken("sas-token");
      ArgumentCaptor<RequestRetryOptions> retry =
          ArgumentCaptor.forClass(RequestRetryOptions.class);
      verify(builder).retryOptions(retry.capture());
      assertThat(retry.getValue().getTryTimeoutDuration()).isEqualTo(TIMEOUT);
    }
  }

  private static ADLSStorageCleanupAdapter adapter(
      DataLakeFileSystemClient client, String location, Duration timeout) {
    return new ADLSStorageCleanupAdapter(client, NormalizedURL.from(location), timeout);
  }
}
