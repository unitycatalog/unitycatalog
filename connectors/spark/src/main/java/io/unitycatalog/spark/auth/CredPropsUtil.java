package io.unitycatalog.spark.auth;

import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import io.unitycatalog.spark.auth.storage.AbfsVendedTokenProvider;
import io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider;
import io.unitycatalog.spark.auth.storage.GcsVendedTokenProvider;
import io.unitycatalog.spark.fs.CredScopedFileSystem;
import io.unitycatalog.spark.fs.CredScopedFs;
import java.util.Map;
import java.util.UUID;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.collect.ImmutableMap;

public class CredPropsUtil {
  private CredPropsUtil() {}

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    public T set(String key, String value) {
      builder.put(key, value);
      return self();
    }

    public T uri(String uri) {
      builder.put(UCHadoopConf.UC_URI_KEY, uri);
      return self();
    }

    public T tokenProvider(TokenProvider tokenProvider) {
      // As we can only propagate the properties with prefix 'fs.*' to the FileSystem
      // implementation. So let's add the prefix here.
      tokenProvider
          .configs()
          .forEach((key, value) -> builder.put(UCHadoopConf.UC_AUTH_PREFIX + key, value));
      return self();
    }

    public T uid(String uid) {
      builder.put(UCHadoopConf.UC_CREDENTIALS_UID_KEY, uid);
      return self();
    }

    public T credentialType(String credType) {
      Preconditions.checkArgument(
          UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE.equals(credType)
              || UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE.equals(credType),
          "Invalid credential type '%s', must be either 'path' or 'table'.",
          credType);
      builder.put(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, credType);
      return self();
    }

    public T tableId(String tableId) {
      builder.put(UCHadoopConf.UC_TABLE_ID_KEY, tableId);
      return self();
    }

    public T tableOperation(TableOperation tableOp) {
      builder.put(UCHadoopConf.UC_TABLE_OPERATION_KEY, tableOp.getValue());
      return self();
    }

    public T path(String path) {
      builder.put(UCHadoopConf.UC_PATH_KEY, path);
      return self();
    }

    public T pathOperation(PathOperation pathOp) {
      builder.put(UCHadoopConf.UC_PATH_OPERATION_KEY, pathOp.getValue());
      return self();
    }

    /**
     * Saves the current value of {@code key} from {@code fsImplProps} (falling back to {@code
     * defaultOriginal}) under {@code key + ".original"}, then overrides {@code key} with {@code
     * newValue}. This lets {@link CredScopedFileSystem#newFileSystem} restore the real delegate
     * implementation after the wrapper has been installed.
     */
    public T saveAndOverride(
        Map<String, String> fsImplProps, String key, String defaultOriginal, String newValue) {
      builder.put(key + ".original", fsImplProps.getOrDefault(key, defaultOriginal));
      builder.put(key, newValue);
      return self();
    }

    protected abstract T self();

    public Map<String, String> build() {
      return builder.build();
    }
  }

  private static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

    S3PropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      // Common properties for S3.
      set("fs.s3a.path.style.access", "true");
      set("fs.s3.impl.disable.cache", "true");
      set("fs.s3a.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CredScopedFs.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected S3PropsBuilder self() {
      return this;
    }
  }

  private static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {

    GcsPropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      // Common properties for GCS.
      set("fs.gs.create.items.conflict.check.enable", "true");
      set("fs.gs.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  private static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {

    AbfsPropsBuilder(boolean credScopedFsEnabled, Map<String, String> fsImplProps) {
      set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      set("fs.abfs.impl.disable.cache", "true");
      set("fs.abfss.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            fsImplProps,
            "fs.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            CredScopedFileSystem.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.Abfs",
            CredScopedFs.class.getName());
        saveAndOverride(
            fsImplProps,
            "fs.AbstractFileSystem.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.Abfss",
            CredScopedFs.class.getName());
      }
    }

    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }

  private static Map<String, String> s3FixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    return new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
        .set("fs.s3a.access.key", awsCred.getAccessKeyId())
        .set("fs.s3a.secret.key", awsCred.getSecretAccessKey())
        .set("fs.s3a.session.token", awsCred.getSessionToken())
        .build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder =
        new S3PropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId())
            .set(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey())
            .set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());

    // For the static credential case, nullable expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> s3TableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> s3PathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> gsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    Long expirationTime =
        tempCreds.getExpirationTime() == null ? Long.MAX_VALUE : tempCreds.getExpirationTime();
    return new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken())
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    GcpOauthToken gcpToken = tempCreds.getGcpOauthToken();
    GcsPropsBuilder builder =
        new GcsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(UCHadoopConf.GCS_INIT_OAUTH_TOKEN, gcpToken.getOauthToken());

    // For the static credential case, nullable expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCHadoopConf.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> gsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> gsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> abfsFixedCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    return new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
        .set(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, azureSas.getSasToken())
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder(credScopedFsEnabled, fsImplProps)
            .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName())
            .uri(uri)
            .tokenProvider(tokenProvider)
            .uid(UUID.randomUUID().toString())
            .set(UCHadoopConf.AZURE_INIT_SAS_TOKEN, azureSas.getSasToken());

    // For the static credential case, nullable expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCHadoopConf.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    return builder;
  }

  private static Map<String, String> abfsTableTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> abfsPathTempCredProps(
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, fsImplProps, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  /**
   * Builds the Hadoop configuration properties needed to access a table's storage location.
   *
   * @param renewCredEnabled when {@code true}, configures a vended-token provider that
   *     automatically refreshes credentials before expiry; when {@code false}, embeds the initial
   *     credentials as static keys.
   * @param credScopedFsEnabled when {@code true}, overrides {@code fs.<scheme>.impl} with {@link
   *     CredScopedFileSystem} so that filesystem instances are reused per credential scope rather
   *     than created anew for every file access.
   * @param fsImplProps the existing table/path properties, used to read any previously configured
   *     {@code fs.<scheme>.impl} values before they are overridden by {@link CredScopedFileSystem}.
   */
  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String scheme,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (renewCredEnabled) {
          return s3TableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsTableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTableTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }

  /**
   * Creates table credential properties from a delta-rest StorageCredential. This converts the
   * generic config map from the new API into Hadoop FS properties.
   */
  public static Map<String, String> createTableCredPropsFromStorageCredential(
      boolean renewCredEnabled,
      String scheme,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      io.unitycatalog.client.deltarest.model.StorageCredential cred) {
    if (cred == null || cred.getConfig() == null || cred.getConfig().isEmpty()) {
      return ImmutableMap.of();
    }
    // Convert StorageCredential to TemporaryCredentials and delegate
    Map<String, String> config = cred.getConfig();
    TemporaryCredentials tempCreds = new TemporaryCredentials();
    if (config.containsKey("s3.access-key-id")) {
      AwsCredentials aws = new AwsCredentials();
      aws.setAccessKeyId(config.get("s3.access-key-id"));
      aws.setSecretAccessKey(config.get("s3.secret-access-key"));
      aws.setSessionToken(config.get("s3.session-token"));
      tempCreds.setAwsTempCredentials(aws);
    } else if (config.containsKey("gcs.oauth-token")) {
      GcpOauthToken gcp = new GcpOauthToken();
      gcp.setOauthToken(config.get("gcs.oauth-token"));
      tempCreds.setGcpOauthToken(gcp);
    } else if (config.containsKey("azure.sas-token")) {
      AzureUserDelegationSAS azure = new AzureUserDelegationSAS();
      azure.setSasToken(config.get("azure.sas-token"));
      tempCreds.setAzureUserDelegationSas(azure);
    }
    if (cred.getExpirationTimeMs() != null) {
      tempCreds.setExpirationTime(cred.getExpirationTimeMs());
    }
    return createTableCredProps(
        renewCredEnabled,
        false,
        new java.util.HashMap<>(),
        scheme,
        uri,
        tokenProvider,
        tableId,
        TableOperation.READ_WRITE,
        tempCreds);
  }

  /**
   * Builds the Hadoop configuration properties needed to access an external storage path.
   *
   * @param renewCredEnabled when {@code true}, configures a vended-token provider that
   *     automatically refreshes credentials before expiry; when {@code false}, embeds the initial
   *     credentials as static keys.
   * @param credScopedFsEnabled when {@code true}, overrides {@code fs.<scheme>.impl} with {@link
   *     CredScopedFileSystem} so that filesystem instances are reused per credential scope rather
   *     than created anew for every file access.
   * @param fsImplProps the existing table/path properties, used to read any previously configured
   *     {@code fs.<scheme>.impl} values before they are overridden by {@link CredScopedFileSystem}.
   */
  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Map<String, String> fsImplProps,
      String scheme,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (renewCredEnabled) {
          return s3PathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsPathTempCredProps(
              credScopedFsEnabled, fsImplProps, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, fsImplProps, tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }
}
