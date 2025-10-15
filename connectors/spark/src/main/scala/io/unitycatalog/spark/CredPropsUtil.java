package io.unitycatalog.spark;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.auth.AbfsVendedTokenProvider;
import io.unitycatalog.spark.auth.AwsVendedTokenProvider;
import java.util.Map;
import java.util.UUID;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.collect.ImmutableMap;

public class CredPropsUtil {
  private CredPropsUtil() {
  }

  public static final ThreadLocal<Boolean> LOAD_DELTA_CATALOG = ThreadLocal.withInitial(() -> true);
  public static final ThreadLocal<Boolean> DELTA_CATALOG_LOADED =
      ThreadLocal.withInitial(() -> false);

  public abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    public T set(String key, String value) {
      builder.put(key, value);
      return self();
    }

    public T uri(String uri) {
      builder.put(UCHadoopConf.UC_URI_KEY, uri);
      return self();
    }

    public T token(String token) {
      builder.put(UCHadoopConf.UC_TOKEN_KEY, token);
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
          "Credential type must be one of 'path' or 'table");
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

    protected abstract T self();

    public Map<String, String> build() {
      return builder.build();
    }
  }

  public static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

    public S3PropsBuilder() {
      // Common properties for S3.
      set("fs.s3a.path.style.access", "true");
      set("fs.s3.impl.disable.cache", "true");
      set("fs.s3a.impl.disable.cache", "true");
    }

    @Override
    protected S3PropsBuilder self() {
      return this;
    }
  }

  public static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {
    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  public static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {
    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }

  public static Map<String, String> s3FixedCredProps(TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    return new S3PropsBuilder()
        .set("fs.s3a.access.key", awsCred.getAccessKeyId())
        .set("fs.s3a.secret.key", awsCred.getSecretAccessKey())
        .set("fs.s3a.session.token", awsCred.getSessionToken())
        .build();
  }

  public static Map<String, String> s3TableTempCredProps(
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder = new S3PropsBuilder()
        .set(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName())
        .uri(uri)
        .token(token)
        .uid(UUID.randomUUID().toString())
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .set(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId())
        .set(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey())
        .set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());

    // For the static credential case, null expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }

    builder.set("fs.s3a.access.key", awsCred.getAccessKeyId());
    builder.set("fs.s3a.secret.key", awsCred.getSecretAccessKey());
    builder.set("fs.s3a.session.token", awsCred.getSessionToken());

    return builder.build();
  }

  public static Map<String, String> s3PathTempCredProps(
      String uri,
      String token,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds
  ) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder = new S3PropsBuilder()
        .set(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName())
        .uri(uri)
        .token(token)
        .uid(UUID.randomUUID().toString())
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .set(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId())
        .set(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey())
        .set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());

    // For the static credential case, null expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(tempCreds.getExpirationTime()));
    }


    return builder.build();
  }


  public static Map<String, String> gsProps(TemporaryCredentials tempCreds) {
    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    return new GcsPropsBuilder()
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken())
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY,
            String.valueOf(tempCreds.getExpirationTime()))
        .set("fs.gs.create.items.conflict.check.enable", "false")
        .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
        .set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName())
        .set("fs.gs.impl.disable.cache", "true")
        .build();
  }

  public static Map<String, String> abfsProps(TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS sas = tempCreds.getAzureUserDelegationSas();
    return new AbfsPropsBuilder()
        .set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS")
        .set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true")
        .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName())
        .set(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, sas.getSasToken())
        .set("fs.abfs.impl.disable.cache", "true")
        .set("fs.abfss.impl.disable.cache", "true")
        .build();
  }

  public static Map<String, String> createTableCredProps(
      boolean useFixedCred,
      String scheme,
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (useFixedCred) {
          return s3FixedCredProps(tempCreds);
        } else {
          return s3TableTempCredProps(uri, token, tableId, tableOp, tempCreds);
        }
      case "gs":
        return gsProps(tempCreds);
      case "abfss":
      case "abfs":
        return abfsProps(tempCreds);
      default:
        return ImmutableMap.of();
    }
  }

  public static Map<String, String> createPathCredProps(
      boolean useFixedCred,
      String scheme,
      String uri,
      String token,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (useFixedCred) {
          return s3FixedCredProps(tempCreds);
        } else {
          return s3PathTempCredProps(uri, token, path, pathOp, tempCreds);
        }
      case "gs":
        return gsProps(tempCreds);
      case "abfss":
      case "abfs":
        return abfsProps(tempCreds);
      default:
        return ImmutableMap.of();
    }
  }
}
