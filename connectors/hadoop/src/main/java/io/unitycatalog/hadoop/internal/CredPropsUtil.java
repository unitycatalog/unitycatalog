package io.unitycatalog.hadoop.internal;

import static io.unitycatalog.hadoop.internal.UCHadoopConf.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static io.unitycatalog.hadoop.internal.UCHadoopConf.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static io.unitycatalog.hadoop.internal.UCHadoopConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;

/**
 * Internal utility that builds cloud-provider specific Hadoop configuration properties for Unity
 * Catalog vended credentials.
 *
 * <p><b>This is an internal class and is not part of the public API.</b> Use {@link
 * UCCredentialHadoopConfs} instead.
 */
public class CredPropsUtil {
  private CredPropsUtil() {}

  private static final String CRED_SCOPED_FS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem";
  private static final String CRED_SCOPED_AFS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFs";
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";
  private static final String GCS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String ABFS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider";
  private static final String GCS_ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  private static final String GCS_ACCESS_TOKEN_EXPIRATION_KEY =
      "fs.gs.auth.access.token.expiration";
  private static final String ABFS_FIXED_SAS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final HashMap<String, String> builder = new HashMap<>();

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
     * Saves the current value of {@code key} from {@code hadoopProps} (falling back to {@code
     * defaultOriginal}) under {@code key + ".original"}, then overrides {@code key} with {@code
     * newValue}. This lets CredScopedFileSystem#newFileSystem restore the real delegate
     * implementation after the wrapper has been installed.
     */
    public T saveAndOverride(
        Configuration hadoopConf, String key, String defaultOriginal, String newValue) {
      builder.put(key + ".original", hadoopConf.get(key, defaultOriginal));
      builder.put(key, newValue);
      return self();
    }

    protected abstract T self();

    public Map<String, String> build() {
      return Collections.unmodifiableMap(new HashMap<>(builder));
    }
  }

  private static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

    S3PropsBuilder(boolean credScopedFsEnabled, Configuration hadoopConf) {
      // Common properties for S3.
      set("fs.s3a.path.style.access", "true");
      set("fs.s3.impl.disable.cache", "true");
      set("fs.s3a.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            hadoopConf,
            "fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CRED_SCOPED_AFS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A",
            CRED_SCOPED_AFS_CLASS);
      }
    }

    @Override
    protected S3PropsBuilder self() {
      return this;
    }
  }

  private static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {

    GcsPropsBuilder(boolean credScopedFsEnabled, Configuration hadoopConf) {
      // Common properties for GCS.
      set("fs.gs.create.items.conflict.check.enable", "true");
      set("fs.gs.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            hadoopConf,
            "fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            CRED_SCOPED_AFS_CLASS);
      }
    }

    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  private static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {

    AbfsPropsBuilder(boolean credScopedFsEnabled, Configuration hadoopConf) {
      set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      set("fs.abfs.impl.disable.cache", "true");
      set("fs.abfss.impl.disable.cache", "true");

      if (credScopedFsEnabled) {
        saveAndOverride(
            hadoopConf,
            "fs.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
            CRED_SCOPED_FS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.AbstractFileSystem.abfs.impl",
            "org.apache.hadoop.fs.azurebfs.Abfs",
            CRED_SCOPED_AFS_CLASS);
        saveAndOverride(
            hadoopConf,
            "fs.AbstractFileSystem.abfss.impl",
            "org.apache.hadoop.fs.azurebfs.Abfss",
            CRED_SCOPED_AFS_CLASS);
      }
    }

    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }

  private static Map<String, String> s3FixedCredProps(
      boolean credScopedFsEnabled, Configuration hadoopConf, TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    return new S3PropsBuilder(credScopedFsEnabled, hadoopConf)
        .set("fs.s3a.access.key", awsCred.getAccessKeyId())
        .set("fs.s3a.secret.key", awsCred.getSecretAccessKey())
        .set("fs.s3a.session.token", awsCred.getSessionToken())
        .build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder =
        new S3PropsBuilder(credScopedFsEnabled, hadoopConf)
            .set(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS)
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
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> s3PathTempCredProps(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> gsFixedCredProps(
      boolean credScopedFsEnabled, Configuration hadoopConf, TemporaryCredentials tempCreds) {
    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    Long expirationTime =
        tempCreds.getExpirationTime() == null ? Long.MAX_VALUE : tempCreds.getExpirationTime();
    return new GcsPropsBuilder(credScopedFsEnabled, hadoopConf)
        .set(GCS_ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken())
        .set(GCS_ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    GcpOauthToken gcpToken = tempCreds.getGcpOauthToken();
    GcsPropsBuilder builder =
        new GcsPropsBuilder(credScopedFsEnabled, hadoopConf)
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GCS_VENDED_TOKEN_PROVIDER_CLASS)
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
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> gsPathTempCredProps(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> abfsFixedCredProps(
      boolean credScopedFsEnabled, Configuration hadoopConf, TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    return new AbfsPropsBuilder(credScopedFsEnabled, hadoopConf)
        .set(ABFS_FIXED_SAS_TOKEN_KEY, azureSas.getSasToken())
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder(credScopedFsEnabled, hadoopConf)
            .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, ABFS_VENDED_TOKEN_PROVIDER_CLASS)
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
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> abfsPathTempCredProps(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(credScopedFsEnabled, hadoopConf, uri, tokenProvider, tempCreds)
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
   * @param credScopedFsEnabled when {@code true}, overrides {@code fs.<scheme>.impl} with
   *     CredScopedFileSystem so that filesystem instances are reused per credential scope rather
   *     than created anew for every file access.
   * @param hadoopConf the engine's existing Hadoop configuration, used to read any previously
   *     configured {@code fs.<scheme>.impl} values before they are overridden by
   *     CredScopedFileSystem.
   */
  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
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
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsTableTempCredProps(
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTableTempCredProps(
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, tableId, tableOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      default:
        return Collections.emptyMap();
    }
  }

  /**
   * Builds the Hadoop configuration properties needed to access an external storage path.
   *
   * @param renewCredEnabled when {@code true}, configures a vended-token provider that
   *     automatically refreshes credentials before expiry; when {@code false}, embeds the initial
   *     credentials as static keys.
   * @param credScopedFsEnabled when {@code true}, overrides {@code fs.<scheme>.impl} with
   *     CredScopedFileSystem so that filesystem instances are reused per credential scope rather
   *     than created anew for every file access.
   * @param hadoopConf the engine's existing Hadoop configuration, used to read any previously
   *     configured {@code fs.<scheme>.impl} values before they are overridden by
   *     CredScopedFileSystem.
   */
  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
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
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return s3FixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsPathTempCredProps(
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return gsFixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsPathTempCredProps(
              credScopedFsEnabled, hadoopConf, uri, tokenProvider, path, pathOp, tempCreds);
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, hadoopConf, tempCreds);
        }
      default:
        return Collections.emptyMap();
    }
  }
}
