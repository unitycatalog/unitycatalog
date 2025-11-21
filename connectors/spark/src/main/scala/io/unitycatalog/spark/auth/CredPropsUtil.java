package io.unitycatalog.spark.auth;

import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static io.unitycatalog.spark.UCHadoopConf.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.UCHadoopConf;
import java.util.Map;
import java.util.UUID;
import org.sparkproject.guava.base.Preconditions;
import org.sparkproject.guava.collect.ImmutableMap;

public class CredPropsUtil {
  private CredPropsUtil() {}

  private static Map<String, String> s3FixedCredProps(TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder =
        new S3PropsBuilder()
            .set("fs.s3a.access.key", awsCred.getAccessKeyId())
            .set("fs.s3a.secret.key", awsCred.getSecretAccessKey())
            .set("fs.s3a.session.token", awsCred.getSessionToken());

    if (tempCreds.getEndpointUrl() != null && !tempCreds.getEndpointUrl().isEmpty()) {
      builder.set("fs.s3a.endpoint", tempCreds.getEndpointUrl());
    }
    return builder.build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      String uri, String token, TemporaryCredentials tempCreds) {
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    S3PropsBuilder builder =
        new S3PropsBuilder()
            .set(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName())
            .uri(uri)
            .token(token)
            .uid(UUID.randomUUID().toString())
            .set(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId())
            .set(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey())
            .set(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());

    // For the static credential case, nullable expiration time is possible.
    if (tempCreds.getExpirationTime() != null) {
      builder.set(
          UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME, String.valueOf(tempCreds.getExpirationTime()));
    }

    if (tempCreds.getEndpointUrl() != null && !tempCreds.getEndpointUrl().isEmpty()) {
      builder.set("fs.s3a.endpoint", tempCreds.getEndpointUrl());
    }

    return builder;
  }

  private static Map<String, String> s3TableTempCredProps(
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> s3PathTempCredProps(
      String uri, String token, String path, PathOperation pathOp, TemporaryCredentials tempCreds) {
    return s3TempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> gsFixedCredProps(TemporaryCredentials tempCreds) {
    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    Long expirationTime =
        tempCreds.getExpirationTime() == null ? Long.MAX_VALUE : tempCreds.getExpirationTime();
    return new GcsPropsBuilder()
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken())
        .set(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      String uri, String token, TemporaryCredentials tempCreds) {
    GcpOauthToken gcpToken = tempCreds.getGcpOauthToken();
    GcsPropsBuilder builder =
        new GcsPropsBuilder()
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName())
            .uri(uri)
            .token(token)
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
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> gsPathTempCredProps(
      String uri, String token, String path, PathOperation pathOp, TemporaryCredentials tempCreds) {
    return gcsTempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  private static Map<String, String> abfsFixedCredProps(TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    return new AbfsPropsBuilder()
        .set(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, azureSas.getSasToken())
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      String uri, String token, TemporaryCredentials tempCreds) {
    AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder()
            .set(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName())
            .uri(uri)
            .token(token)
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
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE)
        .tableId(tableId)
        .tableOperation(tableOp)
        .build();
  }

  private static Map<String, String> abfsPathTempCredProps(
      String uri, String token, String path, PathOperation pathOp, TemporaryCredentials tempCreds) {
    return abfsTempCredPropsBuilder(uri, token, tempCreds)
        .credentialType(UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE)
        .path(path)
        .pathOperation(pathOp)
        .build();
  }

  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      String scheme,
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (renewCredEnabled) {
          return s3TableTempCredProps(uri, token, tableId, tableOp, tempCreds);
        } else {
          return s3FixedCredProps(tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsTableTempCredProps(uri, token, tableId, tableOp, tempCreds);
        } else {
          return gsFixedCredProps(tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTableTempCredProps(uri, token, tableId, tableOp, tempCreds);
        } else {
          return abfsFixedCredProps(tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }

  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      String scheme,
      String uri,
      String token,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        if (renewCredEnabled) {
          return s3PathTempCredProps(uri, token, path, pathOp, tempCreds);
        } else {
          return s3FixedCredProps(tempCreds);
        }
      case "gs":
        if (renewCredEnabled) {
          return gsPathTempCredProps(uri, token, path, pathOp, tempCreds);
        } else {
          return gsFixedCredProps(tempCreds);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsPathTempCredProps(uri, token, path, pathOp, tempCreds);
        } else {
          return abfsFixedCredProps(tempCreds);
        }
      default:
        return ImmutableMap.of();
    }
  }

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

    protected abstract T self();

    public Map<String, String> build() {
      return builder.build();
    }
  }

  private static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

    S3PropsBuilder() {
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

  private static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {

    GcsPropsBuilder() {
      // Common properties for GCS.
      set("fs.gs.create.items.conflict.check.enable", "true");
      set("fs.gs.impl.disable.cache", "true");
    }

    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  private static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {

    AbfsPropsBuilder() {
      set(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      set("fs.abfs.impl.disable.cache", "true");
      set("fs.abfss.impl.disable.cache", "true");
    }

    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }
}
