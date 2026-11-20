package io.unitycatalog.server.service.credential.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
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
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
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
    // Each lookup is capped, so storage that does not answer cannot hold up the request.
    ArgumentCaptor<Consumer<ClientOverrideConfiguration.Builder>> override =
        ArgumentCaptor.forClass(Consumer.class);
    verify(builder, times(2)).overrideConfiguration(override.capture());
    ClientOverrideConfiguration.Builder configuration = ClientOverrideConfiguration.builder();
    override.getValue().accept(configuration);
    assertThat(configuration.build().apiCallTimeout()).contains(Duration.ofSeconds(10));
  }

  private record Failure(RuntimeException error, Duration retryDelay) {}

  @Test
  public void testStorageWithoutRegionUsesDefault() {
    // A successful answer with a blank region carries none, for every caller.
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion(" ").build());
    assertThat(S3BucketRegions.headBucketRegion(s3, "bucket")).isEmpty();
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
  public void testRecheckDoesNotHoldUpRequestsThatHaveARegion() throws Exception {
    // While one request rechecks a region past its lifetime, others get the region reported before
    // rather than wait on the storage.
    CountDownLatch inRecheck = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-test-1").build())
        .thenAnswer(
            invocation -> {
              inRecheck.countDown();
              release.await();
              return HeadBucketResponse.builder().bucketRegion("eu-test-2").build();
            });
    AtomicLong now = new AtomicLong();
    S3BucketRegions rechecking = new S3BucketRegions(() -> builder, now::get);
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    now.addAndGet(S3BucketRegions.REPORTED_REGION_TTL.toMillis());
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<String> recheck =
          pool.submit(() -> rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket"));
      inRecheck.await();
      assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
      release.countDown();
      assertThat(recheck.get(10, TimeUnit.SECONDS)).isEqualTo("eu-test-2");
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
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
        // Refused without the region header, as a proxy or a changed policy may answer, or with a
        // blank one, which carries no region either.
        .thenThrow(s3Exception(403, null))
        .thenThrow(s3Exception(503, ""))
        .thenThrow(s3Exception(403, " "))
        .thenThrow(s3Exception(404, null));
    AtomicLong now = new AtomicLong();
    S3BucketRegions rechecking = new S3BucketRegions(() -> builder, now::get);

    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    now.addAndGet(S3BucketRegions.REPORTED_REGION_TTL.toMillis());
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    // Not asked again until the failure delay has passed.
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    for (int i = 0; i < 4; i++) {
      now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis());
      assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket")).isEqualTo("eu-test-1");
    }
    now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis());
    assertThat(rechecking.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
        .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
    verify(s3, times(7)).headBucket(any(HeadBucketRequest.class));
  }

  @Test
  public void testAwsBucketKeepsItsReportedRegionAndHasNoDefault() {
    // An AWS bucket shares the lookup: its region is cached, and a failed recheck keeps it.
    when(s3.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().bucketRegion("eu-central-1").build())
        .thenThrow(s3Exception(503, null));
    AtomicLong now = new AtomicLong();
    S3BucketRegions aws = new S3BucketRegions(() -> builder, now::get);
    assertThat(aws.getAwsRegion("bucket")).isEqualTo("eu-central-1");
    assertThat(aws.getAwsRegion("bucket")).isEqualTo("eu-central-1");
    now.addAndGet(S3BucketRegions.REPORTED_REGION_TTL.toMillis());
    assertThat(aws.getAwsRegion("bucket")).isEqualTo("eu-central-1");
    verify(s3, times(2)).headBucket(any(HeadBucketRequest.class));
    // The storage is not overridden: the request goes to AWS S3.
    verify(builder, never()).endpointOverride(any());
    // After the failed recheck it backs off: AWS is asked again only once the delay has passed.
    now.addAndGet(S3BucketRegions.FAILURE_RETRY_DELAY.toMillis() - 1);
    assertThat(aws.getAwsRegion("bucket")).isEqualTo("eu-central-1");
    verify(s3, times(2)).headBucket(any(HeadBucketRequest.class));
    now.incrementAndGet();
    assertThat(aws.getAwsRegion("bucket")).isEqualTo("eu-central-1");
    verify(s3, times(3)).headBucket(any(HeadBucketRequest.class));
    // If the reported region is evicted during the backoff, the bucket is asked again, and the
    // failure is an error rather than the default of S3-compatible storage.
    aws.evictReportedRegion(/* s3Endpoint= */ null, "bucket");
    assertThatThrownBy(() -> aws.getAwsRegion("bucket"))
        .isInstanceOfSatisfying(
            BaseException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INTERNAL));
    verify(s3, times(4)).headBucket(any(HeadBucketRequest.class));

    // Without a known region it has no default: transient failures and answers without a region
    // fail every time, and the bucket is asked again on the next request.
    for (RuntimeException failure :
        List.<RuntimeException>of(
            s3Exception(503, null), SdkClientException.create("Connection refused"))) {
      S3Client client = mock(S3Client.class);
      when(builder.build()).thenReturn(client);
      when(client.headBucket(any(HeadBucketRequest.class))).thenThrow(failure);
      S3BucketRegions fresh = new S3BucketRegions(() -> builder, now::get);
      for (int i = 0; i < 2; i++) {
        assertThatThrownBy(() -> fresh.getAwsRegion("other"))
            .isInstanceOfSatisfying(
                BaseException.class,
                e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INTERNAL));
      }
      verify(client, times(2)).headBucket(any(HeadBucketRequest.class));
    }
    S3Client noRegion = mock(S3Client.class);
    when(builder.build()).thenReturn(noRegion);
    when(noRegion.headBucket(any(HeadBucketRequest.class)))
        .thenReturn(HeadBucketResponse.builder().build());
    assertThatThrownBy(() -> new S3BucketRegions(() -> builder, now::get).getAwsRegion("other"))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.FAILED_PRECONDITION));
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

    // So does a failure to build the client: S3-compatible storage backs off with the default, and
    // an AWS bucket, which has no default, gets a retriable error.
    when(builder.build()).thenThrow(new IllegalStateException("cannot build the client"));
    S3BucketRegions unbuildable = new S3BucketRegions(() -> builder, now::get);
    for (int i = 0; i < 2; i++) {
      assertThat(unbuildable.getS3CompatibleRegion(S3_ENDPOINT, "bucket"))
          .isEqualTo(S3BucketRegions.DEFAULT_S3_COMPATIBLE_REGION);
    }
    verify(builder, times(2)).build();
    assertThatThrownBy(() -> unbuildable.getAwsRegion("bucket"))
        .isInstanceOfSatisfying(
            BaseException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INTERNAL));
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
