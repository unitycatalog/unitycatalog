package io.unitycatalog.server.cleanup;

import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

/** Lists and deletes bounded batches under one exact GCS task prefix. */
public final class GCSStorageCleanupAdapter implements StorageCleanupAdapter {
  // The GCS JSON batch API accepts at most 100 calls in one request.
  private static final int MAX_BATCH_SIZE = 100;

  private final Storage storage;
  private final String bucket;
  private final String key;
  private final String keyPrefix;
  private final String storageRoot;
  private boolean exactKeyScheduled;

  /** Creates an adapter with a new timeout-configured client for this attempt. */
  public GCSStorageCleanupAdapter(
      Credentials credentials, NormalizedURL location, Duration requestTimeout) {
    this(storageOptions(credentials, requestTimeout).getService(), location);
  }

  GCSStorageCleanupAdapter(Storage storage, NormalizedURL location) {
    this.storage = Objects.requireNonNull(storage, "storage");
    URI uri = Objects.requireNonNull(location, "location").toUri();
    if (UriScheme.fromURI(uri) != UriScheme.GS
        || uri.getHost() == null
        || uri.getHost().isBlank()) {
      throw new IllegalArgumentException("GCS cleanup requires a GCS storage location");
    }
    String path = uri.getPath();
    String objectKey = path == null ? "" : path.replaceFirst("^/+", "");
    if (objectKey.isEmpty()) {
      throw new IllegalArgumentException("GCS cleanup requires a bucket and object prefix");
    }
    this.bucket = uri.getHost();
    this.key = objectKey;
    this.keyPrefix = key + "/";
    this.storageRoot = "gs://" + bucket + "/";
  }

  @Override
  public List<String> listBatch(int maxFiles) {
    if (maxFiles <= 0) {
      throw new IllegalArgumentException("Cleanup batch size must be positive");
    }
    if (exactKeyScheduled) {
      return List.of();
    }
    int limit = Math.min(maxFiles, MAX_BATCH_SIZE);
    Page<Blob> page =
        storage.list(bucket, BlobListOption.prefix(keyPrefix), BlobListOption.pageSize(limit));
    List<String> locations =
        StreamSupport.stream(page.getValues().spliterator(), false)
            .limit(limit)
            .map(this::toLocation)
            .toList();
    if (locations.isEmpty()) {
      exactKeyScheduled = true;
      return List.of(storageRoot + key);
    }
    return locations;
  }

  @Override
  public void deleteBatch(List<String> locations) {
    if (locations.isEmpty()) {
      return;
    }
    if (locations.size() > MAX_BATCH_SIZE) {
      throw new IllegalArgumentException("GCS cleanup batch cannot exceed 100 objects");
    }
    List<BlobId> blobIds = locations.stream().map(this::toBlobId).toList();
    StorageBatch batch = storage.batch();
    List<StorageBatchResult<Boolean>> results = blobIds.stream().map(batch::delete).toList();
    batch.submit();
    results.forEach(StorageBatchResult::get);
  }

  private String toLocation(Blob blob) {
    BlobId blobId = blob.getBlobId();
    if (!bucket.equals(blobId.getBucket()) || !blobId.getName().startsWith(keyPrefix)) {
      throw new IllegalStateException("GCS listed an object outside the cleanup prefix");
    }
    return storageRoot + blobId.getName();
  }

  private BlobId toBlobId(String location) {
    if (!location.startsWith(storageRoot)) {
      throw new IllegalArgumentException("GCS cleanup cannot delete outside its task prefix");
    }
    String objectKey = location.substring(storageRoot.length());
    if (!objectKey.equals(key) && !objectKey.startsWith(keyPrefix)) {
      throw new IllegalArgumentException("GCS cleanup cannot delete outside its task prefix");
    }
    return BlobId.of(bucket, objectKey);
  }

  static HttpStorageOptions storageOptions(Credentials credentials, Duration requestTimeout) {
    int timeoutMillis = timeoutMillis(requestTimeout);
    org.threeten.bp.Duration timeout = org.threeten.bp.Duration.ofMillis(timeoutMillis);
    RetrySettings retrySettings =
        HttpStorageOptions.getDefaultInstance().getRetrySettings().toBuilder()
            .setInitialRpcTimeout(timeout)
            .setMaxRpcTimeout(timeout)
            .setRpcTimeoutMultiplier(1.0)
            .setTotalTimeout(timeout)
            .build();
    HttpTransportOptions transport =
        HttpTransportOptions.newBuilder()
            .setConnectTimeout(timeoutMillis)
            .setReadTimeout(timeoutMillis)
            .build();
    return HttpStorageOptions.newBuilder()
        .setCredentials(Objects.requireNonNull(credentials, "credentials"))
        .setRetrySettings(retrySettings)
        .setTransportOptions(transport)
        .build();
  }

  private static int timeoutMillis(Duration requestTimeout) {
    try {
      long millis = requestTimeout.toMillis();
      if (millis <= 0 || millis > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "GCS request timeout must be between 1 and 2147483647 milliseconds");
      }
      return (int) millis;
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("GCS request timeout is too large", e);
    }
  }
}
