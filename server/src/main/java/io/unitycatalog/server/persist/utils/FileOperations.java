package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.AzureUserDelegationSAS;
import io.unitycatalog.server.model.GcpOauthToken;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.StorageCredentialVendor;
import io.unitycatalog.server.service.credential.azure.ADLSLocationUtils;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.UriScheme;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

/**
 * Single entry point for all storage/file access in the server. Covers both directory lifecycle
 * management for managed storage locations (create/delete) and credential-vended Iceberg {@link
 * FileIO} construction used by the Iceberg REST catalog.
 */
public class FileOperations {

  private final StorageCredentialVendor storageCredentialVendor;
  private final Map<NormalizedURL, String> s3BucketRegionMap;

  public FileOperations(
      StorageCredentialVendor storageCredentialVendor, ServerProperties serverProperties) {
    this.storageCredentialVendor = storageCredentialVendor;
    this.s3BucketRegionMap =
        serverProperties.getS3Configurations().entrySet().stream()
            .filter(entry -> entry.getValue().getRegion() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getRegion()));
  }

  /** Delete entire directory recursively. Note that currently it does nothing for cloud FS */
  public static void deleteDirectory(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      // Directory deletion for local paths is handled by SimpleLocalFileIO.
      case FILE, NULL -> SimpleLocalFileIO.deleteDirectory(url.toString());
      // Currently we can NOT delete the path in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  /** Create a directory for storage location. Note that currently it does nothing for cloud FS */
  public static void createStorageLocationDir(NormalizedURL url) {
    switch (UriScheme.fromURI(url.toUri())) {
      case FILE, NULL -> createLocalDirectory(url);
      // Currently we can NOT create the directory in cloud storage. We will update this in future
      // when UC OSS begins using the hadoopfs libraries.
      case S3 -> {}
      case GS -> {}
      case ABFS, ABFSS -> {}
    }
  }

  private static void createLocalDirectory(NormalizedURL url) {
    Path dirPath = Paths.get(url.toUri());
    // Check if directory already exists
    if (Files.exists(dirPath)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + dirPath);
    }
    // Create the directory
    try {
      Files.createDirectories(dirPath);
    } catch (IOException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + dirPath, e);
    }
  }

  /**
   * Returns an Iceberg {@link FileIO} for reading the given location. Local paths use {@link
   * SimpleLocalFileIO}; cloud paths use a credential-vended {@link ResolvingFileIO}.
   */
  // TODO: Cache fileIOs
  public FileIO getFileIO(NormalizedURL path) {
    return getFileIO(path, CredentialContext.READ_ONLY);
  }

  /** Returns a FileIO configured for the requested storage privileges. */
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
   * Builds the Iceberg FileIO configuration (credentials, region, token expiry) for the given
   * location by vending temporary storage credentials for it. Returns an empty map for local
   * (file://) paths, which need no cloud credentials.
   *
   * @param path the normalized storage location to vend credentials and build config for
   */
  public Map<String, String> getFileIOConfig(NormalizedURL path) {
    return getFileIOConfig(path, CredentialContext.READ_ONLY);
  }

  /** Builds FileIO configuration using the requested storage privileges. */
  public Map<String, String> getFileIOConfig(
      NormalizedURL path, Set<CredentialContext.Privilege> privileges) {
    return getFileIOConfig(path, privileges, Optional.empty());
  }

  /**
   * Builds FileIO configuration that also tells the client where to renew the vended credentials.
   *
   * <p>The endpoint is only meaningful in a configuration handed to a REST client: Iceberg's
   * clients resolve it against their catalog URI, which the server's own FileIO does not have, so
   * the server's internal callers must leave it empty.
   *
   * @param credentialsEndpoint path of the loadCredentials endpoint for this table, relative to the
   *     catalog URI, or empty for a configuration the server builds for itself
   */
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
          path, cred.getAwsTempCredentials(), cred.getExpirationTime(), credentialsEndpoint);
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
      Long expirationTime,
      Optional<String> credentialsEndpoint) {
    // TODO: if region isn't configured, use HEAD bucket to figure out
    String s3Region = s3BucketRegionMap.get(path.getStorageBase());
    if (s3Region == null) {
      // s3BucketRegionMap has no entry for this bucket (Map.get returns null on a miss). Guard
      // here with a clear message rather than letting Map.copyOf throw an opaque
      // NullPointerException below.
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "No S3 region configured for bucket: " + path.getStorageBase());
    }
    Map<String, String> config = new HashMap<>();
    config.put(S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.getAccessKeyId());
    config.put(S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.getSecretAccessKey());
    config.put(S3FileIOProperties.SESSION_TOKEN, awsCredentials.getSessionToken());
    config.put(AwsClientProperties.CLIENT_REGION, s3Region);
    if (expirationTime != null) {
      // Without this, an Iceberg client cannot tell when the session it was handed dies, so it
      // neither renews ahead of the expiry nor treats the credential as expiring at all.
      config.put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(expirationTime));
    }
    credentialsEndpoint.ifPresent(
        endpoint -> config.put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, endpoint));
    return Map.copyOf(config);
  }
}
