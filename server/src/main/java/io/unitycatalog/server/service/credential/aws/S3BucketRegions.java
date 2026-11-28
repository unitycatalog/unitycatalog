package io.unitycatalog.server.service.credential.aws;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Regions of buckets, read from an anonymous HeadBucket. AWS S3 and S3-compatible storage such as
 * MinIO return the region in the {@code x-amz-bucket-region} header, even on access-denied (403)
 * responses, so no keys or bucket permissions are needed.
 *
 * <p>AWS S3 and S3-compatible storage share the lookup: a reported region is cached per storage and
 * bucket and asked again after {@link #REPORTED_REGION_TTL}, a failed recheck keeps it, and one
 * lookup runs per bucket at a time. They differ in two ways:
 *
 * <ul>
 *   <li>When no region is known, S3-compatible storage commonly expects {@link
 *       #DEFAULT_S3_COMPATIBLE_REGION}, while an AWS bucket has no default, so its lookup fails.
 *   <li>A recheck answered with 404 (no such bucket) drops the reported region on S3-compatible
 *       storage, where the same bucket name may be created again with another region. An AWS bucket
 *       keeps it, as for any failed recheck.
 * </ul>
 */
public class S3BucketRegions {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3BucketRegions.class);
  private static final String BUCKET_REGION_HEADER = "x-amz-bucket-region";
  private static final Duration DISCOVERY_TIMEOUT = Duration.ofSeconds(10);
  // How long a failed lookup answers with the region reported before, or on S3-compatible storage
  // with the default, before the storage is asked again, so that storage that is down does not
  // hold up every request for the discovery timeout. An AWS bucket with no reported region does
  // not back off: its lookup fails, and runs again on the next request.
  static final Duration FAILURE_RETRY_DELAY = Duration.ofSeconds(30);
  // How long an answer without a region keeps the default before the storage is asked again, so
  // that a region configured later is picked up without a restart.
  static final Duration NO_REGION_RECHECK_DELAY = Duration.ofMinutes(5);

  /** The form of a region name, as {@code assume_role.sts_region} takes it. */
  private static final String REGION_PATTERN = "[A-Za-z0-9._-]+";

  /** The region S3-compatible storage commonly expects when it is not configured with one. */
  public static final String DEFAULT_S3_COMPATIBLE_REGION = Region.US_EAST_1.id();

  private final Supplier<S3ClientBuilder> s3ClientBuilderSupplier;
  // Endpoints come from the credential API and buckets from request paths, so the caches are
  // bounded. A reported region is asked again after REPORTED_REGION_TTL, so that a region fixed at
  // the storage, or an endpoint moved to other storage, is picked up without a restart; it stays in
  // use while that request fails.
  static final Duration REPORTED_REGION_TTL = Duration.ofHours(1);
  private static final int MAX_BUCKETS = 10_000;
  private final Cache<StorageBucket, Reported> reportedRegions;
  // Until when, in epoch milliseconds, a bucket is not asked again: it gets the region reported
  // before, or on S3-compatible storage the default.
  private final Cache<StorageBucket, Long> retryAfterMs;
  private final Map<StorageBucket, CompletableFuture<String>> lookups = new ConcurrentHashMap<>();
  private final LongSupplier currentTimeMillis;

  /** A bucket, on S3-compatible storage at {@code s3Endpoint}, or on AWS S3 when it is null. */
  private record StorageBucket(String s3Endpoint, String bucket) {
    boolean isAws() {
      return s3Endpoint == null;
    }
  }

  /** A region the storage reported, and when, in epoch milliseconds, to ask it again. */
  private record Reported(String region, long recheckAtMs) {}

  public S3BucketRegions() {
    this(S3Client::builder);
  }

  /**
   * @param s3ClientBuilderSupplier supplies the builder for the anonymous discovery client
   */
  public S3BucketRegions(Supplier<S3ClientBuilder> s3ClientBuilderSupplier) {
    this(s3ClientBuilderSupplier, System::currentTimeMillis);
  }

  /**
   * @param s3ClientBuilderSupplier supplies the builder for the anonymous discovery client
   * @param currentTimeMillis the clock for retrying failed lookups
   */
  S3BucketRegions(
      Supplier<S3ClientBuilder> s3ClientBuilderSupplier, LongSupplier currentTimeMillis) {
    this.s3ClientBuilderSupplier = s3ClientBuilderSupplier;
    this.currentTimeMillis = currentTimeMillis;
    Ticker ticker = () -> TimeUnit.MILLISECONDS.toNanos(currentTimeMillis.getAsLong());
    this.reportedRegions =
        Caffeine.newBuilder().maximumSize(MAX_BUCKETS).executor(Runnable::run).build();
    this.retryAfterMs =
        Caffeine.newBuilder()
            .maximumSize(MAX_BUCKETS)
            .expireAfterWrite(NO_REGION_RECHECK_DELAY)
            .ticker(ticker)
            .executor(Runnable::run)
            .build();
  }

  /**
   * Sends HeadBucket and reads the bucket's region from the response or its error.
   *
   * @param s3Client the client to send it with
   * @param bucket the bucket name
   * @return the region, or empty if a successful response did not carry one
   * @throws S3Exception if the storage answered with an error that carries no region
   * @throws SdkException if the request failed otherwise, such as on a network error
   */
  public static Optional<String> headBucketRegion(S3Client s3Client, String bucket) {
    try {
      // A blank region is none, for every caller.
      return Optional.ofNullable(
              s3Client
                  .headBucket(HeadBucketRequest.builder().bucket(bucket).build())
                  .bucketRegion())
          .filter(region -> !region.isBlank());
    } catch (S3Exception e) {
      AwsErrorDetails details = e.awsErrorDetails();
      Optional<String> region =
          details == null || details.sdkHttpResponse() == null
              ? Optional.empty()
              : details.sdkHttpResponse().firstMatchingHeader(BUCKET_REGION_HEADER);
      // A blank header carries no region, like a missing one: the error, not an answer without one.
      return Optional.of(region.filter(value -> !value.isBlank()).orElseThrow(() -> e));
    }
  }

  /**
   * @param e an error the storage answered with
   * @return whether the same request may succeed later: a server error, a timeout, or throttling
   */
  public static boolean isTransient(S3Exception e) {
    return e.statusCode() >= 500 || e.statusCode() == 408 || e.isThrottlingException();
  }

  /**
   * The region of a bucket on S3-compatible storage. A region the storage reports is cached per S3
   * endpoint and bucket, and asked again after {@link #REPORTED_REGION_TTL}. Otherwise the default
   * region is used. After a failed request or a transient error, the last reported region, or the
   * default, is used and the storage is asked again after {@link #FAILURE_RETRY_DELAY}; a missing
   * bucket (which may be created later) does the same with the default. After an answer without a
   * region, the default is used for {@link #NO_REGION_RECHECK_DELAY}. One lookup runs per bucket at
   * a time.
   *
   * @param s3Endpoint the storage's S3 endpoint
   * @param bucket the bucket name
   * @return the reported region, otherwise {@link #DEFAULT_S3_COMPATIBLE_REGION}
   */
  public String getS3CompatibleRegion(String s3Endpoint, String bucket) {
    return getRegion(new StorageBucket(s3Endpoint, bucket));
  }

  /**
   * The region of an AWS S3 bucket. A region it reports is cached per bucket, and asked again after
   * {@link #REPORTED_REGION_TTL}; while a recheck fails, the reported region stays in use, and the
   * bucket is asked again after {@link #FAILURE_RETRY_DELAY}. A bucket without a known region has
   * no default, so its lookup fails, and it is asked again on the next request. One lookup runs per
   * bucket at a time.
   *
   * @param bucket the bucket name
   * @return the reported region
   * @throws BaseException {@code FAILED_PRECONDITION} if the region cannot be determined (such as
   *     for no such bucket), or {@code INTERNAL} if the lookup failed transiently (retriable)
   */
  public String getAwsRegion(String bucket) {
    return getRegion(new StorageBucket(/* s3Endpoint= */ null, bucket));
  }

  private String getRegion(StorageBucket storageBucket) {
    Optional<String> known = knownRegion(storageBucket);
    if (known.isPresent()) {
      return known.get();
    }
    CompletableFuture<String> lookup = new CompletableFuture<>();
    CompletableFuture<String> running = lookups.putIfAbsent(storageBucket, lookup);
    if (running != null) {
      // Another request is rechecking; a region reported earlier serves meanwhile, so only a
      // bucket with none waits for the storage.
      Reported reported = reportedRegions.getIfPresent(storageBucket);
      if (reported != null) {
        return reported.region();
      }
      try {
        return running.join();
      } catch (CompletionException e) {
        throw e.getCause() instanceof RuntimeException cause ? cause : e;
      }
    }
    try {
      // A lookup may have finished between the check above and this one taking over.
      String region = knownRegion(storageBucket).orElseGet(() -> lookUp(storageBucket));
      lookup.complete(region);
      return region;
    } catch (Throwable t) {
      // Any failure, an Error too, completes the lookup, so no waiter blocks on it.
      lookup.completeExceptionally(t);
      throw t;
    } finally {
      lookups.remove(storageBucket, lookup);
    }
  }

  /**
   * @return the region to use without asking the storage, or empty if it is time to ask
   */
  private Optional<String> knownRegion(StorageBucket storageBucket) {
    long now = currentTimeMillis.getAsLong();
    Reported reported = reportedRegions.getIfPresent(storageBucket);
    if (reported != null && now < reported.recheckAtMs()) {
      return Optional.of(reported.region());
    }
    Long retryAfter = retryAfterMs.getIfPresent(storageBucket);
    if (retryAfter != null && now < retryAfter) {
      // The two caches evict apart: an AWS bucket has no default, so without the reported region
      // it is asked again rather than given the default of S3-compatible storage.
      // The region already read: a second read could miss an entry evicted in between.
      if (reported != null) {
        return Optional.of(reported.region());
      }
      if (!storageBucket.isAws()) {
        return Optional.of(DEFAULT_S3_COMPATIBLE_REGION);
      }
    }
    return Optional.empty();
  }

  private String lastReportedOrDefault(StorageBucket storageBucket) {
    Reported reported = reportedRegions.getIfPresent(storageBucket);
    return reported == null ? DEFAULT_S3_COMPATIBLE_REGION : reported.region();
  }

  private String lookUp(StorageBucket storageBucket) {
    String s3Endpoint = storageBucket.s3Endpoint();
    String bucket = storageBucket.bucket();
    // Discovery runs outside any map lock, so a slow storage does not serialize requests for other
    // buckets. The client is built in the try, so a failure to build it is handled as a failed
    // request.
    try (S3Client s3Client = discoveryClient(storageBucket)) {
      Optional<String> region = headBucketRegion(s3Client, bucket);
      if (region.isEmpty() || region.get().isBlank()) {
        return withoutRegion(storageBucket, "no region header");
      }
      // The header signs requests and reaches clients, so it takes the form sts_region does.
      if (!region.get().matches(REGION_PATTERN)) {
        return withoutRegion(storageBucket, "a malformed region header");
      }
      reportedRegions.put(
          storageBucket,
          new Reported(
              region.get(), currentTimeMillis.getAsLong() + REPORTED_REGION_TTL.toMillis()));
      retryAfterMs.invalidate(storageBucket);
      return region.get();
    } catch (S3Exception e) {
      if (storageBucket.isAws()) {
        return awsFailed(storageBucket, e, isTransient(e));
      }
      if (isTransient(e) || e.statusCode() == 404) {
        LOGGER.warn("Could not read the region of bucket {} from {}", bucket, s3Endpoint, e);
        if (e.statusCode() == 404) {
          reportedRegions.invalidate(storageBucket);
        }
        return failed(storageBucket);
      }
      if (reportedRegions.getIfPresent(storageBucket) != null) {
        // Refused without a region where one was reported: keep it, as for a failed request.
        LOGGER.warn("Could not read the region of bucket {} from {}", bucket, s3Endpoint, e);
        return failed(storageBucket);
      }
      // The storage answered without a region, so it is not configured with one.
      return withoutRegion(storageBucket, e.getMessage());
    } catch (BaseException e) {
      // An AWS lookup that already failed with its own error, as for no region.
      throw e;
    } catch (RuntimeException e) {
      // An SdkException, such as a network error, or a failure building the request.
      if (storageBucket.isAws()) {
        return awsFailed(storageBucket, e, /* isTransient= */ true);
      }
      LOGGER.warn("Could not reach {} to read the region of bucket {}", s3Endpoint, bucket, e);
      return failed(storageBucket);
    }
  }

  /** The anonymous client to read a bucket's region with, on its storage. */
  private S3Client discoveryClient(StorageBucket storageBucket) {
    // A region the SDK needs to build the client; the anonymous request is not signed with it.
    S3ClientBuilder builder =
        s3ClientBuilderSupplier
            .get()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .overrideConfiguration(c -> c.apiCallTimeout(DISCOVERY_TIMEOUT));
    if (!storageBucket.isAws()) {
      builder.endpointOverride(URI.create(storageBucket.s3Endpoint())).forcePathStyle(true);
    }
    return builder.build();
  }

  /**
   * An AWS bucket has no default region: a region reported earlier stays in use, as on
   * S3-compatible storage, and otherwise the lookup fails, and runs again on the next request.
   */
  private String awsFailed(StorageBucket storageBucket, Throwable cause, boolean isTransient) {
    return awsFailed(storageBucket, cause, isTransient, /* reason= */ null);
  }

  private String awsFailed(
      StorageBucket storageBucket, Throwable cause, boolean isTransient, String reason) {
    // Read once: the entry could be evicted before a second read.
    Reported reported = reportedRegions.getIfPresent(storageBucket);
    if (reported != null) {
      LOGGER.warn("Could not read the region of AWS S3 bucket {}", storageBucket.bucket(), cause);
      backOff(storageBucket);
      return reported.region();
    }
    String location = "s3://" + storageBucket.bucket();
    if (isTransient) {
      // Transient failures are retried silently by the client, so log the underlying S3 error.
      LOGGER.warn("Transient failure resolving the AWS region for S3 bucket {}", location, cause);
      throw new BaseException(
          ErrorCode.INTERNAL,
          "Could not resolve the AWS region for S3 bucket " + location + "; retry the request.",
          cause);
    }
    // The cause is not surfaced to the caller, so log the underlying S3 error for diagnosis.
    LOGGER.warn(
        "Could not resolve the AWS region for S3 bucket {}: {}",
        location,
        cause != null
            ? cause.getMessage()
            : reason != null ? reason : "HeadBucket returned no region");
    // FAILED_PRECONDITION, not INVALID_ARGUMENT: the request is well-formed and the caller cannot
    // fix this; the region is unresolvable until an operator configures s3.bucketPath.N and
    // s3.region.N (both map to HTTP 400, so this is a clearer label at no wire cost).
    throw new BaseException(
        ErrorCode.FAILED_PRECONDITION,
        "Could not resolve the AWS region for S3 bucket "
            + location
            + "; configure the matching s3.bucketPath.N and s3.region.N.",
        cause);
  }

  private String failed(StorageBucket storageBucket) {
    backOff(storageBucket);
    return lastReportedOrDefault(storageBucket);
  }

  /** Drops the region reported for {@code bucket}, as a cache eviction would. */
  @VisibleForTesting
  void evictReportedRegion(String s3Endpoint, String bucket) {
    reportedRegions.invalidate(new StorageBucket(s3Endpoint, bucket));
  }

  private void backOff(StorageBucket storageBucket) {
    retryAfterMs.put(storageBucket, currentTimeMillis.getAsLong() + FAILURE_RETRY_DELAY.toMillis());
  }

  private String withoutRegion(StorageBucket storageBucket, String reason) {
    if (storageBucket.isAws()) {
      return awsFailed(storageBucket, /* cause= */ null, /* isTransient= */ false, reason);
    }
    LOGGER.info(
        "{} reports no region for bucket {}; using {} and asking again in {}: {}",
        storageBucket.s3Endpoint(),
        storageBucket.bucket(),
        DEFAULT_S3_COMPATIBLE_REGION,
        NO_REGION_RECHECK_DELAY,
        reason);
    reportedRegions.invalidate(storageBucket);
    retryAfterMs.put(
        storageBucket, currentTimeMillis.getAsLong() + NO_REGION_RECHECK_DELAY.toMillis());
    return DEFAULT_S3_COMPATIBLE_REGION;
  }
}
