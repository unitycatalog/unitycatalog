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
import java.util.Map;
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
    return switch (UriScheme.fromURI(path.toUri())) {
      // Local paths are served by SimpleLocalFileIO (backed by java.nio + iceberg-core). We
      // deliberately do NOT route these through ResolvingFileIO: it resolves the file:// scheme to
      // Iceberg's HadoopFileIO, which requires hadoop-client-runtime on the classpath. The server
      // only depends on hadoop-client-api, and SimpleLocalFileIO covers the local read and
      // directory operations we need without that heavy runtime dependency.
      case FILE, NULL -> new SimpleLocalFileIO();
      case S3, GS, ABFS, ABFSS -> {
        ResolvingFileIO fileio = new ResolvingFileIO();
        fileio.initialize(getFileIOConfig(path));
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
    UriScheme scheme = UriScheme.fromURI(path.toUri());
    if (scheme == UriScheme.FILE || scheme == UriScheme.NULL) {
      // Local (file://) paths need no cloud credentials, so short-circuit before vending: the
      // scheme is known here and vending would do a needless external-location lookup for local
      // tables.
      return Map.of();
    }

    // FIXME!! privileges are defaulted to READ only here for now as Iceberg REST impl doesn't
    //  support write
    TemporaryCredentials cred =
        storageCredentialVendor.vendCredential(path, Set.of(CredentialContext.Privilege.SELECT));
    if (cred.getAzureUserDelegationSas() != null) {
      return getADLSConfig(path, cred.getAzureUserDelegationSas());
    } else if (cred.getGcpOauthToken() != null) {
      return getGCSConfig(cred.getGcpOauthToken(), cred.getExpirationTime());
    } else if (cred.getAwsTempCredentials() != null) {
      return getS3Config(path, cred.getAwsTempCredentials());
    } else {
      // Cloud vend returned no recognized credential type. This should not happen for a cloud
      // scheme, so fail loudly rather than silently returning an empty (credential-less) config
      // that would later surface as an opaque access-denied error.
      throw new BaseException(
          ErrorCode.INTERNAL,
          "No recognized storage credential was vended for location: " + path);
    }
  }

  private Map<String, String> getADLSConfig(
      NormalizedURL path, AzureUserDelegationSAS azureUserDelegationSAS) {
    ADLSLocationUtils.ADLSLocationParts locationParts = ADLSLocationUtils.parseLocation(path);
    // NOTE: when fileio caching is implemented, need to set/deal with expiry here
    return Map.of(
        AzureProperties.ADLS_SAS_TOKEN_PREFIX + locationParts.account(),
        azureUserDelegationSAS.getSasToken());
  }

  private Map<String, String> getGCSConfig(GcpOauthToken gcpOauthToken, Long expirationTime) {
    if (expirationTime != null) {
      return Map.of(
          GCPProperties.GCS_OAUTH2_TOKEN,
          gcpOauthToken.getOauthToken(),
          GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
          Long.toString(expirationTime));
    } else {
      return Map.of(GCPProperties.GCS_OAUTH2_TOKEN, gcpOauthToken.getOauthToken());
    }
  }

  private Map<String, String> getS3Config(NormalizedURL path, AwsCredentials awsCredentials) {
    // TODO: if region isn't configured, use HEAD bucket to figure out
    String s3Region = s3BucketRegionMap.get(path.getStorageBase());
    if (s3Region == null) {
      // s3BucketRegionMap has no entry for this bucket (Map.get returns null on a miss). Guard
      // here with a clear message rather than letting Map.of throw an opaque NullPointerException
      // below.
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "No S3 region configured for bucket: " + path.getStorageBase());
    }
    return Map.of(
        S3FileIOProperties.ACCESS_KEY_ID, awsCredentials.getAccessKeyId(),
        S3FileIOProperties.SECRET_ACCESS_KEY, awsCredentials.getSecretAccessKey(),
        S3FileIOProperties.SESSION_TOKEN, awsCredentials.getSessionToken(),
        AwsClientProperties.CLIENT_REGION, s3Region);
  }
}
