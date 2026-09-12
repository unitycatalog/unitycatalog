package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.NormalizedURL.S3Location;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Lists and deletes bounded batches under one exact S3 task prefix. */
public final class S3StorageCleanupAdapter implements StorageCleanupAdapter {
  private static final int MAX_BATCH_SIZE = 1000;

  private final S3Client client;
  private final String bucket;
  private final String key;
  private final String keyPrefix;
  private final String storageRoot;
  private final AwsRequestOverrideConfiguration requestOverride;
  private boolean exactKeyScheduled;

  /** Creates an adapter that owns and closes the supplied client. */
  public S3StorageCleanupAdapter(S3Client client, NormalizedURL location, Duration requestTimeout) {
    this.client = Objects.requireNonNull(client, "client");
    S3Location s3Location;
    try {
      s3Location = Objects.requireNonNull(location, "location").toS3Location();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("S3 cleanup requires an S3 storage location");
    }
    if (s3Location.key().isEmpty()) {
      throw new IllegalArgumentException("S3 cleanup requires a bucket and object prefix");
    }
    this.bucket = s3Location.bucket();
    this.key = s3Location.key();
    this.keyPrefix = key + "/";
    this.storageRoot = "s3://" + bucket + "/";
    this.requestOverride = requestOverride(requestTimeout);
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
    ListObjectsV2Request request =
        ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(keyPrefix)
            .maxKeys(limit)
            .overrideConfiguration(requestOverride)
            .build();
    List<String> locations =
        client.listObjectsV2(request).contents().stream()
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
      throw new IllegalArgumentException("S3 cleanup batch cannot exceed 1000 objects");
    }
    List<ObjectIdentifier> objects = locations.stream().map(this::toObjectIdentifier).toList();
    DeleteObjectsRequest request =
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(objects).build())
            .overrideConfiguration(requestOverride)
            .build();
    DeleteObjectsResponse response = client.deleteObjects(request);
    if (response.hasErrors()) {
      String errors =
          response.errors().stream()
              .map(error -> error.key() + " (" + error.code() + ")")
              .collect(Collectors.joining(", "));
      throw SdkClientException.create("S3 object deletion failed: " + errors);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private String toLocation(S3Object object) {
    if (!object.key().startsWith(keyPrefix)) {
      throw new IllegalStateException("S3 listed an object outside the cleanup prefix");
    }
    return storageRoot + object.key();
  }

  private ObjectIdentifier toObjectIdentifier(String location) {
    if (!location.startsWith(storageRoot)) {
      throw new IllegalArgumentException("S3 cleanup cannot delete outside its task prefix");
    }
    String objectKey = location.substring(storageRoot.length());
    if (!objectKey.equals(key) && !objectKey.startsWith(keyPrefix)) {
      throw new IllegalArgumentException("S3 cleanup cannot delete outside its task prefix");
    }
    return ObjectIdentifier.builder().key(objectKey).build();
  }

  private static AwsRequestOverrideConfiguration requestOverride(Duration requestTimeout) {
    try {
      if (requestTimeout.toMillis() <= 0) {
        throw new IllegalArgumentException("S3 request timeout must be at least one millisecond");
      }
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("S3 request timeout is too large", e);
    }
    return AwsRequestOverrideConfiguration.builder()
        .apiCallTimeout(requestTimeout)
        .apiCallAttemptTimeout(requestTimeout)
        .build();
  }
}
