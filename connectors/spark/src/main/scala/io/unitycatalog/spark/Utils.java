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
import io.unitycatalog.spark.auth.AwsVendedTokenProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.sparkproject.guava.collect.ImmutableMap;

public class Utils {
  private Utils() {
  }

  public static final ThreadLocal<Boolean> LOAD_DELTA_CATALOG = ThreadLocal.withInitial(() -> true);
  public static final ThreadLocal<Boolean> DELTA_CATALOG_LOADED =
      ThreadLocal.withInitial(() -> false);

  public static Map<String, String> s3TableCredProps(
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    Map<String, String> map = new HashMap<>();
    // Set to use the AwsVendedTokenProvider
    map.put(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());

    // Set the unity catalog URI and token.
    map.put(UCHadoopConf.UC_URI_KEY, uri);
    map.put(UCHadoopConf.UC_TOKEN_KEY, token);
    map.put(UCHadoopConf.UC_CREDENTIALS_UID_KEY, UUID.randomUUID().toString());

    // Set the temporary credentials requests to unity catalog.
    map.put(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    map.put(UCHadoopConf.UC_TABLE_ID_KEY, tableId);
    map.put(UCHadoopConf.UC_TABLE_OPERATION_KEY, tableOp.toString());

    // Set the initialized temporary credential.
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    map.put(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId());
    map.put(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey());
    map.put(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());
    map.put(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME,
        String.valueOf(tempCreds.getExpirationTime()));

    map.put("fs.s3a.access.key", awsCred.getAccessKeyId());
    map.put("fs.s3a.secret.key", awsCred.getSecretAccessKey());
    map.put("fs.s3a.session.token", awsCred.getSessionToken());

    map.put("fs.s3a.path.style.access", "true");
    map.put("fs.s3.impl.disable.cache", "true");
    map.put("fs.s3a.impl.disable.cache", "true");

    return ImmutableMap.copyOf(map);
  }

  public static Map<String, String> s3PathCredProps(
      String uri,
      String token,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds
  ) {
    Map<String, String> map = new HashMap<>();

    // Set to use the AwsVendedTokenProvider
    map.put(UCHadoopConf.S3A_CREDENTIALS_PROVIDER, AwsVendedTokenProvider.class.getName());

    // Set the unity catalog URI and token.
    map.put(UCHadoopConf.UC_URI_KEY, uri);
    map.put(UCHadoopConf.UC_TOKEN_KEY, token);
    map.put(UCHadoopConf.UC_CREDENTIALS_UID_KEY, UUID.randomUUID().toString());

    // Set the temporary credentials requests to unity catalog.
    map.put(UCHadoopConf.UC_CREDENTIALS_TYPE_KEY, UCHadoopConf.UC_CREDENTIALS_TYPE_PATH_VALUE);
    map.put(UCHadoopConf.UC_PATH_KEY, path);
    map.put(UCHadoopConf.UC_PATH_OPERATION_KEY, pathOp.toString());

    // Set the initialized temporary credential.
    AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
    map.put(UCHadoopConf.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId());
    map.put(UCHadoopConf.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey());
    map.put(UCHadoopConf.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());
    map.put(UCHadoopConf.S3A_INIT_CRED_EXPIRED_TIME,
        String.valueOf(tempCreds.getExpirationTime()));

    map.put("fs.s3a.access.key", awsCred.getAccessKeyId());
    map.put("fs.s3a.secret.key", awsCred.getSecretAccessKey());
    map.put("fs.s3a.session.token", awsCred.getSessionToken());

    map.put("fs.s3a.path.style.access", "true");
    map.put("fs.s3.impl.disable.cache", "true");
    map.put("fs.s3a.impl.disable.cache", "true");

    return ImmutableMap.copyOf(map);
  }


  public static Map<String, String> gsProps(TemporaryCredentials tempCreds) {
    Map<String, String> map = new HashMap<>();

    GcpOauthToken gcpOauthToken = tempCreds.getGcpOauthToken();
    map.put(GcsVendedTokenProvider.ACCESS_TOKEN_KEY, gcpOauthToken.getOauthToken());
    map.put(GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY,
        String.valueOf(tempCreds.getExpirationTime()));
    map.put("fs.gs.create.items.conflict.check.enable", "false");
    map.put("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    map.put("fs.gs.auth.access.token.provider", GcsVendedTokenProvider.class.getName());
    map.put("fs.gs.impl.disable.cache", "true");

    return ImmutableMap.copyOf(map);
  }

  public static Map<String, String> abfsProps(TemporaryCredentials tempCreds) {
    Map<String, String> map = new HashMap<>();

    AzureUserDelegationSAS sas = tempCreds.getAzureUserDelegationSas();
    map.put(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
    map.put(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
    map.put(FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, AbfsVendedTokenProvider.class.getName());
    map.put(AbfsVendedTokenProvider.ACCESS_TOKEN_KEY, sas.getSasToken());
    map.put("fs.abfs.impl.disable.cache", "true");
    map.put("fs.abfss.impl.disable.cache", "true");

    return ImmutableMap.copyOf(map);
  }

  public static Map<String, String> createTableCredProps(
      String scheme,
      String uri,
      String token,
      String tableId,
      TableOperation tableOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        return s3TableCredProps(uri, token, tableId, tableOp, tempCreds);
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
      String scheme,
      String uri,
      String token,
      String path,
      PathOperation pathOp,
      TemporaryCredentials tempCreds) {
    switch (scheme) {
      case "s3":
        return s3PathCredProps(uri, token, path, pathOp, tempCreds);
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
