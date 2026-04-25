package io.unitycatalog.spark.auth;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.util.Map;
import java.util.stream.Collectors;

/** Compatibility wrapper for the shared UC credential property builder. */
public class CredPropsUtil {
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
                Map.Entry::getKey, entry -> toSparkProviderClassName(entry.getValue())));
  }

  private static String toSparkProviderClassName(String value) {
    if (value.equals(io.unitycatalog.client.storage.AwsVendedTokenProvider.class.getName())) {
      return io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider.class.getName();
    }
    if (value.equals(io.unitycatalog.client.storage.GcsVendedTokenProvider.class.getName())) {
      return io.unitycatalog.spark.auth.storage.GcsVendedTokenProvider.class.getName();
    }
    if (value.equals(io.unitycatalog.client.storage.AbfsVendedTokenProvider.class.getName())) {
      return io.unitycatalog.spark.auth.storage.AbfsVendedTokenProvider.class.getName();
    }
    return value;
  }
}
