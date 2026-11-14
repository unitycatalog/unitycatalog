package io.unitycatalog.server.teststorage;

import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Creates buckets through the S3 API, for storage that has no admin tool for it. */
final class S3Buckets {

  private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);

  private S3Buckets() {}

  /**
   * Creates {@code bucket} if it does not exist yet, retrying while the storage is still starting:
   * on connection failures and server errors. Other errors, such as a wrong key, fail at once.
   *
   * @param s3Endpoint the S3 API endpoint
   * @param accessKeyId the access key of an identity that may create buckets
   * @param secretAccessKey the secret paired with {@code accessKeyId}
   * @param bucket the bucket name
   */
  static void create(String s3Endpoint, String accessKeyId, String secretAccessKey, String bucket) {
    long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
    try (S3Client client = client(s3Endpoint, accessKeyId, secretAccessKey)) {
      while (true) {
        try {
          client.createBucket(b -> b.bucket(bucket));
          return;
        } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
          // Some storage answers a repeated create of one's own bucket with BucketAlreadyExists.
          return;
        } catch (SdkException e) {
          if (!isStarting(e) || System.nanoTime() > deadline) {
            throw new IllegalStateException("Could not create bucket " + bucket, e);
          }
          sleep();
        }
      }
    }
  }

  /**
   * Waits until the storage stores a new object, creating {@code bucket} for it. Some storage
   * accepts S3 requests before it can allocate space for objects, and a first write then waits
   * until it can; this keeps that wait out of the tests.
   *
   * @param s3Endpoint the S3 API endpoint
   * @param accessKeyId the access key of an identity that may create buckets and write objects
   * @param secretAccessKey the secret paired with {@code accessKeyId}
   * @param bucket the bucket to write the object to
   */
  static void awaitWritable(
      String s3Endpoint, String accessKeyId, String secretAccessKey, String bucket) {
    create(s3Endpoint, accessKeyId, secretAccessKey, bucket);
    long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
    try (S3Client client = client(s3Endpoint, accessKeyId, secretAccessKey)) {
      while (true) {
        try {
          client.putObject(b -> b.bucket(bucket).key("ready"), RequestBody.fromString("ready"));
          return;
        } catch (SdkException e) {
          if (!isStarting(e)) {
            throw e;
          }
          if (System.nanoTime() > deadline) {
            throw new IllegalStateException("Storage does not store objects yet", e);
          }
          sleep();
        }
      }
    }
  }

  /** Whether the storage may still be starting: it cannot be reached or answers a server error. */
  private static boolean isStarting(SdkException e) {
    return e instanceof SdkClientException
        || (e instanceof S3Exception s3 && s3.statusCode() >= 500);
  }

  private static S3Client client(String s3Endpoint, String accessKeyId, String secretAccessKey) {
    return S3Client.builder()
        .endpointOverride(URI.create(s3Endpoint))
        .region(Region.US_EAST_1)
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
        .build();
  }

  private static void sleep() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for the storage", e);
    }
  }
}
