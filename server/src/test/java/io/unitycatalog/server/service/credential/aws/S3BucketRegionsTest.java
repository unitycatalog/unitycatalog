package io.unitycatalog.server.service.credential.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3BucketRegionsTest {
  private static final String S3_ENDPOINT = "http://storage.example.com:9000";

  private final S3Client s3 = mock(S3Client.class);
  private final S3ClientBuilder builder = mock(S3ClientBuilder.class);
  private final S3BucketRegions regions = new S3BucketRegions(() -> builder);

  {
    when(builder.credentialsProvider(any())).thenReturn(builder);
    when(builder.region(any())).thenReturn(builder);
    when(builder.endpointOverride(any())).thenReturn(builder);
    when(builder.forcePathStyle(any())).thenReturn(builder);
    when(builder.overrideConfiguration(any(Consumer.class))).thenReturn(builder);
    when(builder.build()).thenReturn(s3);
  }

  private static S3Exception s3Exception(String region) {
    return s3Exception(403, region);
  }

  private static S3Exception s3Exception(int statusCode, String region) {
    SdkHttpResponse.Builder http = SdkHttpResponse.builder();
    if (region != null) {
      http.putHeader("x-amz-bucket-region", region);
    }
    return (S3Exception)
        S3Exception.builder()
            .statusCode(statusCode)
            .awsErrorDetails(AwsErrorDetails.builder().sdkHttpResponse(http.build()).build())
            .build();
  }

  @Test
  public void testReportedRegionIsUsedAndCachedPerStorageAndBucket() {
    // Storage such as MinIO reports its region even when it denies the anonymous request.
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(s3Exception("eu-test-1"))
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-test-2").build());

    for (int i = 0; i < 2; i++) {
      assertThat(regions.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    }
    assertThat(regions.getS3CompatibleRegion(S3_ENDPOINT, "other")).isEqualTo("eu-test-2");
    verify(s3, times(2)).headBucket(any(HeadBucketRequest.class));
    verify(builder, times(2)).endpointOverride(URI.create(S3_ENDPOINT));
    verify(builder, times(2)).forcePathStyle(true);
  }

  private record Failure(RuntimeException error, Duration retryDelay) {}

  @Test
  public void testStorageWithoutRegionUsesDefault() {
    // The default region is used, and the storage is asked again after a delay: a failed
    // request, a transient error (server error, timeout) or a missing bucket after the retry
    // delay, and an answer without a region after the longer recheck delay.
    for (Failure failure :
        List.of(
            new Failure(s3Exception(null), S3BucketRegions.NO_REGION_RECHECK_DELAY),
            // A blank or malformed header counts as none: it would sign requests and reach clients.
            new Failure(s3Exception(""), S3BucketRegions.NO_REGION_RECHECK_DELAY),
            new Failure(s3Exception("us east 1"), S3BucketRegions.NO_REGION_RECHECK_DELAY),
            new Failure(s3Exception(503, null), S3BucketRegions.FAILURE_RETRY_DELAY),
            new Failure(s3Exception(408, null), S3BucketRegions.FAILURE_RETRY_DELAY),
            new Failure(s3Exception(404, null), S3BucketRegions.FAILURE_RETRY_DELAY),
            new Failure(
                SdkClientException.create("Connection refused"),
                S3BucketRegions.FAILURE_RETRY_DELAY))) {
      S3Client client = mock(S3Client.class);
      when(builder.build()).thenReturn(client);
      when(client.headBucket(any(HeadBucketRequest.class))).thenThrow(failure.error());
      AtomicLong now = new AtomicLong();
      S3BucketRegions fresh = new S3BucketRegions(() -> builder, now::get);

      for (int i = 0; i < 2; i++) {
        assertThat(fresh.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
            .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
      }
      verify(client, times(1)).headBucket(any(HeadBucketRequest.class));
      now.addAndGet(failure.retryDelay().toMillis() - 1);
      fresh.getS3CompatibleRegion(S3_ENDPOINT, "bucket");
      verify(client, times(1)).headBucket(any(HeadBucketRequest.class));
      now.addAndGet(1);
      assertThat(fresh.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
          .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
      verify(client, times(2)).headBucket(any(HeadBucketRequest.class));
    }
  }

  @Test
  public void testConcurrentRequestsShareOneLookup() throws Exception {
    // While one lookup waits on the storage, other requests for the bucket wait for its answer.
    CountDownLatch inLookup = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenAnswer(
            invocation -> {
              inLookup.countDown();
              release.await();
              return HeadBucketResponse.builder().bucketRegion("eu-test-1").build();
            });
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<String> first =
          pool.submit(() -> regions.getS3CompatibleRegion(S3_ENDPOINT, "bucket"));
      inLookup.await();
      Future<String> second =
          pool.submit(() -> regions.getS3CompatibleRegion(S3_ENDPOINT, "bucket"));
      release.countDown();
      assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo("eu-test-1");
      assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo("eu-test-1");
    } finally {
      pool.shutdownNow();
    }
    verify(s3, times(1)).headBucket(any(HeadBucketRequest.class));
  }

  @Test
  public void testReportedRegionIsAskedAgainAfterItsLifetime() {
    // A region fixed at the storage, or an endpoint moved to other storage, is picked up.
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-test-1").build())
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-test-2").build());
    AtomicLong now = new AtomicLong();
    S3BucketRegions expiring = new S3BucketRegions(() -> builder, now::get);

    assertThat(expiring.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    now.addAndGet(S3BucketRegions.REPORTED_REGION_TTL.toMillis() - 1);
    assertThat(expiring.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    now.addAndGet(1);
    assertThat(expiring.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-2");
    verify(s3, times(2)).headBucket(any(HeadBucketRequest.class));
  }

  @Test
  public void testFailedRecheckKeepsTheReportedRegion() {
    // A recheck that fails keeps the region the storage reported; a missing bucket drops it.
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-test-1").build())
        .thenThrow(s3Exception(503, null))
        .thenThrow(SdkClientException.create("connection refused"))
        // Refused without the region header, as a proxy or a changed policy may answer.
        .thenThrow(s3Exception(403, null))
        .thenThrow(s3Exception(404, null));
    AtomicLong now = new AtomicLong();
    S3BucketRegions rechecking = new S3BucketRegions(() -> builder, now::get);

    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    now.addAndGet(S3BucketRegions.REPORTED_REGION_TTL.toMillis());
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    // Not asked again until the failure delay has passed.
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    for (int i = 0; i < 2; i++) {
      now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis());
      assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    }
    now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis());
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
        .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
    verify(s3, times(5)).headBucket(any(HeadBucketRequest.class));
  }

  @Test
  public void testFailureOutsideTheSdkBacksOff() {
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(new IllegalArgumentException("not a valid request"));
    AtomicLong now = new AtomicLong();
    S3BucketRegions retrying = new S3BucketRegions(() -> builder, now::get);

    for (int i = 0; i < 2; i++) {
      assertThat(retrying.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
          .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
    }
    verify(s3, times(1)).headBucket(any(HeadBucketRequest.class));
  }

  @Test
  public void testTransientErrorDoesNotPinTheDefaultRegion() {
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenThrow(s3Exception(503, null))
        .thenThrow(s3Exception("eu-test-1"));
    AtomicLong now = new AtomicLong();
    S3BucketRegions retrying = new S3BucketRegions(() -> builder, now::get);

    assertThat(retrying.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
        .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
    now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis());
    assertThat(retrying.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
  }
}
