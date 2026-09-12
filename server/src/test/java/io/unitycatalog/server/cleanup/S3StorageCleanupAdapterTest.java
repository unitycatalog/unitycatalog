package io.unitycatalog.server.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.utils.NormalizedURL;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3StorageCleanupAdapterTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(3);
  private static final String PREFIX = "root/table/";
  private static final String LOCATION = "s3://bucket/root/table";

  private S3Client client;
  private S3StorageCleanupAdapter adapter;

  @BeforeEach
  void setUp() {
    client = mock(S3Client.class);
    adapter = new S3StorageCleanupAdapter(client, NormalizedURL.from(LOCATION), TIMEOUT);
  }

  @Test
  void listsOneMaterializedExactBatch() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder()
                .contents(object(PREFIX + "a"), object(PREFIX + "b"), object(PREFIX + "c"))
                .nextContinuationToken("not-used")
                .build());

    assertThat(adapter.listBatch(2))
        .containsExactly("s3://bucket/root/table/a", "s3://bucket/root/table/b");

    ListObjectsV2Request request = captureListRequest();
    assertThat(request.bucket()).isEqualTo("bucket");
    assertThat(request.prefix()).isEqualTo(PREFIX);
    assertThat(request.maxKeys()).isEqualTo(2);
    assertThat(request.continuationToken()).isNull();
    assertTimeouts(request.overrideConfiguration().orElseThrow());
  }

  @Test
  void capsAndValidatesListBatchSize() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(ListObjectsV2Response.builder().contents(object(PREFIX + "a")).build());

    assertThat(adapter.listBatch(1001)).containsExactly(LOCATION + "/a");
    assertThat(captureListRequest().maxKeys()).isEqualTo(1000);
    for (int limit : List.of(0, -1)) {
      assertThatThrownBy(() -> adapter.listBatch(limit))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cleanup batch size must be positive");
    }
    verify(client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  void deletesOnlyExpectedKeysWithTimeouts() {
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(DeleteObjectsResponse.builder().build());

    adapter.deleteBatch(List.of("s3://bucket/root/table/a", "s3://bucket/root/table/nested/b"));

    DeleteObjectsRequest request = captureDeleteRequest();
    assertThat(request.bucket()).isEqualTo("bucket");
    assertThat(request.delete().objects())
        .extracting(object -> object.key())
        .containsExactly("root/table/a", "root/table/nested/b");
    assertTimeouts(request.overrideConfiguration().orElseThrow());
  }

  @Test
  void rejectsDeletionOutsideExactPrefix() {
    for (String location :
        List.of("s3://bucket/root/table-other/file", "s3://other/root/table/file")) {
      assertThatThrownBy(() -> adapter.deleteBatch(List.of(location)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("S3 cleanup cannot delete outside its task prefix");
    }
    assertThatThrownBy(
            () ->
                adapter.deleteBatch(
                    java.util.Collections.nCopies(1001, "s3://bucket/root/table/file")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 cleanup batch cannot exceed 1000 objects");
    verify(client, never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @ParameterizedTest
  @ValueSource(strings = {"%20", "%2A", "%3F", "%25"})
  void preservesEncodedKeys(String encodedKey) {
    String key = "root/" + encodedKey;
    S3StorageCleanupAdapter encodedAdapter =
        new S3StorageCleanupAdapter(client, NormalizedURL.from("s3://bucket/" + key), TIMEOUT);
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(ListObjectsV2Response.builder().contents(object(key + "/child")).build());
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(DeleteObjectsResponse.builder().build());

    List<String> batch = encodedAdapter.listBatch(1);
    encodedAdapter.deleteBatch(batch);

    assertThat(captureListRequest().prefix()).isEqualTo(key + "/");
    assertThat(batch).containsExactly("s3://bucket/" + key + "/child");
    assertThat(captureDeleteRequest().delete().objects())
        .extracting(object -> object.key())
        .containsExactly(key + "/child");
  }

  @Test
  void rejectsListedObjectOutsideExactPrefix() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder().contents(object("root/table-other/file")).build());

    assertThatThrownBy(() -> adapter.listBatch(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("S3 listed an object outside the cleanup prefix");
  }

  @Test
  void reportsEveryPerObjectDeleteError() {
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(
            DeleteObjectsResponse.builder()
                .errors(
                    S3Error.builder().key(PREFIX + "a").code("AccessDenied").build(),
                    S3Error.builder().key(PREFIX + "b").code("InternalError").build())
                .build());

    assertThatThrownBy(
            () ->
                adapter.deleteBatch(
                    List.of("s3://bucket/root/table/a", "s3://bucket/root/table/b")))
        .isInstanceOf(SdkClientException.class)
        .hasMessageContaining("root/table/a (AccessDenied)")
        .hasMessageContaining("root/table/b (InternalError)");
  }

  @Test
  void deletesExactKeyAfterDescendants() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(
            ListObjectsV2Response.builder().contents(object(PREFIX + "child")).build(),
            ListObjectsV2Response.builder().build());
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(DeleteObjectsResponse.builder().build());

    StorageCleanupAttempt.Result result =
        new StorageCleanupAttempt(10, Duration.ofSeconds(30), TIMEOUT).run(adapter);

    assertThat(result).isEqualTo(StorageCleanupAttempt.Result.COMPLETE);
    ArgumentCaptor<DeleteObjectsRequest> requests =
        ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(client, times(2)).deleteObjects(requests.capture());
    assertThat(requests.getAllValues())
        .extracting(request -> request.delete().objects().get(0).key())
        .containsExactly(PREFIX + "child", "root/table");
    verify(client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  void treatsMissingExactKeyAsDeleted() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(ListObjectsV2Response.builder().build());
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(DeleteObjectsResponse.builder().build());

    assertThat(adapter.listBatch(10)).containsExactly(LOCATION);
    adapter.deleteBatch(List.of(LOCATION));
    assertThat(adapter.listBatch(10)).isEmpty();

    assertThat(captureDeleteRequest().delete().objects())
        .extracting(object -> object.key())
        .containsExactly("root/table");
    verify(client).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  void newAttemptRepeatsExactKeyDeleteAfterFailure() {
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(ListObjectsV2Response.builder().build());
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(
            DeleteObjectsResponse.builder()
                .errors(S3Error.builder().key("root/table").code("InternalError").build())
                .build(),
            DeleteObjectsResponse.builder().build());

    List<String> firstBatch = adapter.listBatch(10);
    assertThatThrownBy(() -> adapter.deleteBatch(firstBatch))
        .isInstanceOf(SdkClientException.class);
    S3StorageCleanupAdapter retryAdapter =
        new S3StorageCleanupAdapter(client, NormalizedURL.from(LOCATION), TIMEOUT);
    List<String> retryBatch = retryAdapter.listBatch(10);
    retryAdapter.deleteBatch(retryBatch);

    assertThat(firstBatch).containsExactly(LOCATION);
    assertThat(retryBatch).containsExactly(LOCATION);

    ArgumentCaptor<DeleteObjectsRequest> requests =
        ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(client, times(2)).deleteObjects(requests.capture());
    assertThat(requests.getAllValues())
        .allSatisfy(
            request ->
                assertThat(request.delete().objects())
                    .extracting(object -> object.key())
                    .containsExactly("root/table"));
  }

  @Test
  void propagatesSdkFailure() {
    SdkClientException failure = SdkClientException.create("list failed");
    when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(failure);

    assertThatThrownBy(() -> adapter.listBatch(1)).isSameAs(failure);
  }

  @Test
  void rejectsInvalidLocationAndTimeout() {
    assertThatThrownBy(
            () ->
                new S3StorageCleanupAdapter(
                    client, NormalizedURL.from("gs://bucket/path"), TIMEOUT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 cleanup requires an S3 storage location");
    assertThatThrownBy(
            () -> new S3StorageCleanupAdapter(client, NormalizedURL.from("s3://bucket"), TIMEOUT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 cleanup requires a bucket and object prefix");
    assertThatThrownBy(
            () ->
                new S3StorageCleanupAdapter(
                    client, NormalizedURL.from(LOCATION), Duration.ofNanos(999_999)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 request timeout must be at least one millisecond");
  }

  @Test
  void closesSuppliedClient() {
    adapter.close();

    verify(client).close();
  }

  private ListObjectsV2Request captureListRequest() {
    ArgumentCaptor<ListObjectsV2Request> request =
        ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(client).listObjectsV2(request.capture());
    return request.getValue();
  }

  private DeleteObjectsRequest captureDeleteRequest() {
    ArgumentCaptor<DeleteObjectsRequest> request =
        ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(client).deleteObjects(request.capture());
    return request.getValue();
  }

  private static void assertTimeouts(RequestOverrideConfiguration configuration) {
    assertThat(configuration.apiCallTimeout()).contains(TIMEOUT);
    assertThat(configuration.apiCallAttemptTimeout()).contains(TIMEOUT);
  }

  private static S3Object object(String key) {
    return S3Object.builder().key(key).build();
  }
}
