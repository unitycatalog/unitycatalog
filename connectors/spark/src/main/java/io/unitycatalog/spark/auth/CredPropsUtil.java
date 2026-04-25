package io.unitycatalog.spark.auth;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Map;
import java.util.stream.Collectors;

/** Compatibility wrapper for the shared UC credential property builder. */
public class CredPropsUtil {
  private static final String CLIENT_AWS_PROVIDER =
      "io.unitycatalog.client.storage.AwsVendedTokenProvider";
  private static final String CLIENT_GCS_PROVIDER =
      "io.unitycatalog.client.storage.GcsVendedTokenProvider";
  private static final String CLIENT_ABFS_PROVIDER =
      "io.unitycatalog.client.storage.AbfsVendedTokenProvider";
  private static final String SPARK_AWS_PROVIDER =
      "io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider";
  private static final String SPARK_GCS_PROVIDER =
      "io.unitycatalog.spark.auth.storage.GcsVendedTokenProvider";
  private static final String SPARK_ABFS_PROVIDER =
      "io.unitycatalog.spark.auth.storage.AbfsVendedTokenProvider";
  private static final Map<String, String> SPARK_PROVIDER_NAMES =
      Map.of(
          CLIENT_AWS_PROVIDER, SPARK_AWS_PROVIDER,
          CLIENT_GCS_PROVIDER, SPARK_GCS_PROVIDER,
          CLIENT_ABFS_PROVIDER, SPARK_ABFS_PROVIDER);

  private CredPropsUtil() {}

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
    return withSparkProviderClassNames(
        io.unitycatalog.client.storage.CredPropsUtil.createTableCredProps(
            renewCredEnabled,
            credScopedFsEnabled,
            fsImplProps,
            scheme,
            uri,
            tokenProvider,
            tableId,
            tableOp,
            tempCreds));
  }

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
    return withSparkProviderClassNames(
        io.unitycatalog.client.storage.CredPropsUtil.createPathCredProps(
            renewCredEnabled,
            credScopedFsEnabled,
            fsImplProps,
            scheme,
            uri,
            tokenProvider,
            path,
            pathOp,
            tempCreds));
  }

  private static Map<String, String> withSparkProviderClassNames(Map<String, String> props) {
    return props.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> SPARK_PROVIDER_NAMES.getOrDefault(entry.getValue(), entry.getValue())));
  }
}
