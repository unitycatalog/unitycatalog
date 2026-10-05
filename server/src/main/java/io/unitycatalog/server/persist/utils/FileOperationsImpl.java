package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.utils.CooperativeDeadline;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
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

/** Default {@link FileOperations}: builds credential-vended Iceberg {@link FileIO}s. */
public class FileOperationsImpl implements FileOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileOperationsImpl.class);
  // S3 returns the bucket's region in this response header, including on 301/403 error responses.
  private static final String BUCKET_REGION_HEADER = "x-amz-bucket-region";

  private final StorageCredentialVendor storageCredentialVendor;
  // Per-bucket S3 region: seeded from the configured s3.region.N, then filled by HeadBucket
  // discovery for buckets that have none configured. A discovered region is cached for reuse.
  private final Map<NormalizedURL, String> s3BucketRegionMap;
  // Per-bucket S3-compatible endpoint (MinIO, MCG/NooBaa, ...) from s3.endpointUrl.N.
  private final Map<NormalizedURL, String> s3BucketEndpointMap;
  // Fallback region for S3-compatible buckets that have no s3.region.N configured.
  private final String defaultS3Region;
  private final Supplier<S3ClientBuilder> s3ClientBuilderSupplier;

  public FileOperationsImpl(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this(storageCredentialVendor, serverProperties, S3Client::builder);
  }

  /**
   * Creates file operations that resolve an unconfigured bucket's region with the given S3 client
   * builder; the no-supplier constructor uses {@link S3Client#builder}.
   *
   * @param s3ClientBuilderSupplier supplies the builder for the anonymous region-discovery client
   */
  public FileOperationsImpl(
      StorageCredentialVendor storageCredentialVendor,
      ServerProperties serverProperties,
      Supplier<S3ClientBuilder> s3ClientBuilderSupplier) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.s3ClientBuilderSupplier = s3ClientBuilderSupplier;
    this.s3BucketRegionMap = new ConcurrentHashMap<>();
    this.s3BucketEndpointMap = new HashMap<>();
    serverProperties
        .getS3Configurations()
        .forEach(
            (bucket, config) -> {
              if (config.getRegion() != null) {
                s3BucketRegionMap.put(bucket, config.getRegion());
              }
              String endpoint = s3Endpoint(config);
              if (endpoint != null) {
                s3BucketEndpointMap.put(bucket, endpoint);
              }
            });
    String awsRegion = serverProperties.get(ServerProperties.Property.AWS_REGION);
    this.defaultS3Region =
        awsRegion == null || awsRegion.isEmpty() ? Region.US_EAST_1.id() : awsRegion;
  }

  private static String s3Endpoint(S3StorageConfig config) {
    String endpoint = config.getS3EndpointUrl();
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = config.getEndpointUrl();
    }
    return endpoint == null || endpoint.isEmpty() ? null : endpoint;
  }

  // TODO: Cache fileIOs
  @Override
  public FileIO getFileIO(NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    return switch (UriScheme.fromURI(path.toUri())) {
      // Local paths are served by SimpleLocalFileIO (backed by java.nio + iceberg-core). We
      // deliberately do NOT route these through ResolvingFileIO: it resolves the file:// scheme to
      // Iceberg's HadoopFileIO, which requires hadoop-client-runtime on the classpath. The server
      // only depends on hadoop-client-api, and SimpleLocalFileIO covers the local read and
      // directory operations we need without that heavy runtime dependency.
      case FILE, NULL -> new SimpleLocalFileIO();
      case S3, GS, ABFS, ABFSS -> {
        ResolvingFileIO fileio = new ResolvingFileIO();
        fileio.initialize(getFileIOConfig(path, privileges));
        yield fileio;
      }
    };
  }

  /**
   * Returns fresh, write-enabled prefix operations sharing the attempt's cancellation checks.
   *
   * <p>Cloud cleanup uses the provider's default request settings. Cancellation is checked between
   * batches and does not impose a timeout on an in-flight storage call.
   */
  @Override
  public SupportsPrefixOperations getCleanupFileIO(
      NormalizedURL path, CooperativeDeadline deadline) {
    return switch (UriScheme.fromURI(path.toUri())) {
      case FILE, NULL -> new SimpleLocalFileIO(deadline);
      case S3, GS -> {
        ResolvingFileIO fileIO = new ResolvingFileIO();
        fileIO.initialize(getFileIOConfig(path, CredentialContext.READ_WRITE));
        yield new InterruptiblePrefixOperations(fileIO, path + "/", deadline);
      }
      case ABFS, ABFSS -> {
        ADLSFileIO fileIO = new ADLSFileIO();
        fileIO.initialize(getFileIOConfig(path, CredentialContext.READ_WRITE));
        yield new ADLSPrefixOperations(fileIO, path + "/", deadline);
      }
    };
  }

  @Override
  public Map<String, String> getFileIOConfig(
      NormalizedURL path,
      Set<CredentialContext.Privilege> privileges,
      Optional<String> credentialsEndpoint) {
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      // Local (file://) paths need no cloud credentials, so short-circuit before vending: the
      // scheme is known here and vending would do a needless external-location lookup for local
      // tables.
      return Map.of();
    }

    TemporaryCredentials cred = storageCredentialVendor.vendCredential(path, privileges);
    if (cred.getAzureUserDelegationSas() != null) {
      return getADLSConfig(
          path, cred.getAzureUserDelegationSas(), cred.getExpirationTime(), credentialsEndpoint);
    } else if (cred.getGcpOauthToken() != null) {
      return getGCSConfig(cred.getGcpOauthToken(), cred.getExpirationTime(), credentialsEndpoint);
    } else if (cred.getAwsTempCredentials() != null) {
      return getS3Config(
          path,
          cred.getAwsTempCredentials(),
          cred.getEndpointUrl(),
          cred.getExpirationTime(),
          credentialsEndpoint);
    } else {
      // Cloud vend returned no recognized credential type. This should not happen for a cloud
      // scheme, so fail loudly rather than silently returning an empty (credential-less) config
      // that would later surface as an opaque access-denied error.
      throw new BaseException(
          ErrorCode.INTERNAL, "No recognized storage credential was vended for location: " + path);
    }
  }

  private Map<String, String> getADLSConfig(
      NormalizedURL path,
      AzureUserDelegationSAS azureUserDelegationSAS,
      Long expirationTime,
      Optional<String> credentialsEndpoint) {
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(path);
    Map<String, String> config = new HashMap<>();
    config.put(
        AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(),
        azureUserDelegationSAS.getSasToken());
    if (expirationTime != null) {
      config.put(
          AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + locationParts.account(),
          Long.toString(expirationTime));
    }
    credentialsEndpoint.ifPresent(
        endpoint -> config.put(AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT, endpoint));
    return Map.copyOf(config);
  }

  private Map<String, String> getGCSConfig(
      GcpOauthToken gcpOauthToken, Long expirationTime, Optional<String> credentialsEndpoint) {
    Map<String, String> config = new HashMap<>();
    config.put(GCPProperties.GCS_OAUTH2_TOKEN, gcpOauthToken.getOauthToken());
    if (expirationTime != null) {
      config.put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(expirationTime));
    }
    credentialsEndpoint.ifPresent(
        endpoint -> config.put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, endpoint));
    return Map.copyOf(config);
  }

  private Map<String, String> getS3Config(
      NormalizedURL path,
      AwsCredentials awsCredentials,
      String vendedEndpointUrl,
      Long expirationTime,
      Optional<String> credentialsEndpoint) {
    NormalizedURL storageBase = path.getStorageBase();
    String endpointUrl = s3BucketEndpointMap.get(storageBase);
    if (endpointUrl == null || endpointUrl.isEmpty()) {
      endpointUrl = vendedEndpointUrl;
    }
    boolean customEndpoint = endpointUrl != null && !endpointUrl.isEmpty();
    // Use the configured region if present, otherwise discover it and cache it per bucket.
    // Discovery runs outside any map lock (get + putIfAbsent rather than computeIfAbsent): a bucket
    // whose discovery fails is never cached and so is re-probed on every request, and holding the
    // bin lock across HeadBucket would serialize those probes and pile request threads up on it.
    String s3Region = s3BucketRegionMap.get(storageBase);
    if (s3Region == null && customEndpoint) {
      // HeadBucket discovery targets AWS itself, so it would ask AWS about a bucket that lives on
      // an S3-compatible store. Such stores accept any signing region; use the server default.
      s3Region = defaultS3Region;
    } else if (s3Region == null) {
      s3Region = discoverRegion(storageBase);
      s3BucketRegionMap.putIfAbsent(storageBase, s3Region);
    }
    Map<String, String> config = new HashMap<>();
    config.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.getAccessKeyId());
    config.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.getSecretAccessKey());
    // Static access keys carry no session token, and Map.copyOf rejects null values.
    if (awsCredentials.getSessionToken() != null && !awsCredentials.getSessionToken().isEmpty()) {
      config.put(S3FileIOProperties.SESSION_TOKEN, awsCredentials.getSessionToken());
    }
    config.put(AwsClientProperties.CLIENT_REGION, s3Region);
    if (customEndpoint) {
      // Iceberg S3FileIO honours s3.endpoint; path-style is required for MinIO-style stores.
      config.put(S3FileIOProperties.ENDPOINT, endpointUrl);
      config.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    }
    if (expirationTime != null) {
      // Without this, an Iceberg client cannot tell when the session it was handed dies, so it
      // neither renews ahead of the expiry nor treats the credential as expiring at all.
      config.put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(expirationTime));
    }
    credentialsEndpoint.ifPresent(
        endpoint -> config.put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, endpoint));
    return Map.copyOf(config);
  }

  /**
   * Resolves the AWS region for a bucket via an anonymous HeadBucket. S3 returns the region in the
   * {@code x-amz-bucket-region} header even on cross-region (301) and access-denied (403)
   * responses, so no credentials or bucket permissions are required. A bootstrap region is set only
   * because the SDK needs one to build a client; the anonymous request is not signed against it.
   *
   * @throws BaseException {@code FAILED_PRECONDITION} if the region cannot be determined
   *     (permanent, e.g. no such bucket), or {@code INTERNAL} if discovery failed transiently
   *     (retriable)
   */
  private String discoverRegion(NormalizedURL storageBase) {
    String bucket = storageBase.toUri().getHost();
    try (S3Client s3Client =
        s3ClientBuilderSupplier
            .get()
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .build()) {
      String region =
          s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build()).bucketRegion();
      if (region != null) {
        return region;
      }
      throw permanentRegionError(storageBase, null);
    } catch (S3Exception e) {
      String region = bucketRegionHeader(e);
      if (region != null) {
        return region;
      }
      // A 5xx, throttling, or 408 request-timeout response is transient; any other definitive 4xx
      // without the region header (e.g. no such bucket) means the region cannot be determined.
      if (e.statusCode() >= 500 || e.statusCode() == 408 || e.isThrottlingException()) {
        throw transientRegionError(storageBase, e);
      }
      throw permanentRegionError(storageBase, e);
    } catch (SdkException e) {
      // Network, timeout, or other SDK failures leave the region undetermined; retriable.
      throw transientRegionError(storageBase, e);
    }
  }

  /** The {@code x-amz-bucket-region} header from an error response, or null if it is absent. */
  private static String bucketRegionHeader(S3Exception e) {
    AwsErrorDetails details = e.awsErrorDetails();
    if (details == null || details.sdkHttpResponse() == null) {
      return null;
    }
    return details.sdkHttpResponse().firstMatchingHeader(BUCKET_REGION_HEADER).orElse(null);
  }

  private static BaseException permanentRegionError(NormalizedURL storageBase, Throwable cause) {
    // The cause is not surfaced to the caller, so log the underlying S3 error for diagnosis.
    LOGGER.warn(
        "Could not resolve the AWS region for S3 bucket {}: {}",
        storageBase,
        cause == null ? "HeadBucket returned no region" : cause.getMessage());
    // FAILED_PRECONDITION, not INVALID_ARGUMENT: the request is well-formed and the caller cannot
    // fix this — the region is unresolvable until an operator configures
    // s3.bucketPath.N/s3.region.N
    // (both map to HTTP 400, so this is a clearer label at no wire cost). It is non-retriable.
    return new BaseException(
        ErrorCode.FAILED_PRECONDITION,
        "Could not resolve the AWS region for S3 bucket "
            + storageBase
            + "; configure the matching s3.bucketPath.N and s3.region.N.",
        cause);
  }

  private static BaseException transientRegionError(NormalizedURL storageBase, Throwable cause) {
    // Transient failures are retried silently by the client, so log the underlying S3 error.
    LOGGER.warn(
        "Transient failure resolving the AWS region for S3 bucket {}: {}",
        storageBase,
        cause == null ? "unknown" : cause.getMessage());
    return new BaseException(
        ErrorCode.INTERNAL,
        "Could not resolve the AWS region for S3 bucket " + storageBase + "; retry the request.",
        cause);
  }
}
