package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.CredentialCache;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.hadoop.internal.props.CredPropsBuilder;
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

  /**
   * Factory seam for {@link GenericCredentialFetcher#create(ApiClient, CredId)}, swappable from
   * tests so the fetch methods can be exercised without a real UC server. Test-only; production
   * code must not depend on the swap behavior.
   */
  @FunctionalInterface
  public interface GenericCredentialFetcherFactory {
    GenericCredentialFetcher create(ApiClient apiClient, CredId credId);
  }

  public static volatile GenericCredentialFetcherFactory genericCredFetcherFactory =
      GenericCredentialFetcher::create;

  static final CredentialCache<CredId> initialCredCache =
      CredentialCache.createInitialCredentialCache();

  // Keys used to build the credential-context id (see #credContextId).
  private static final String CRED_CONTEXT_CATALOG_URI_KEY = "catalogUri";
  private static final String CRED_CONTEXT_SCHEME_KEY = "scheme";

  /** Fetches table credentials from the UC REST API and builds Hadoop configuration properties. */
  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String tableId,
      UCCredentialHadoopConfs.TableOperation tableOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new TableCredId(
            credContextId(catalogUri, scheme, tokenProvider), tableId, tableOp.value()));
  }

  /**
   * Fetches Delta table credentials from the UC Delta API and builds Hadoop configuration
   * properties.
   */
  public static Map<String, String> createDeltaTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      UCDeltaTableIdentifier identifier,
      String location,
      UCCredentialHadoopConfs.TableOperation tableOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new DeltaTableCredId(
            credContextId(catalogUri, scheme, tokenProvider),
            identifier,
            tableOp.value(),
            location));
  }

  /**
   * Fetches Delta staging table credentials from the UC Delta API and builds Hadoop configuration
   * properties.
   */
  public static Map<String, String> createDeltaStagingTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String stagingTableId,
      String location,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new DeltaStagingTableCredId(
            credContextId(catalogUri, scheme, tokenProvider), stagingTableId, location));
  }

  /** Fetches path credentials from the UC REST API and builds Hadoop configuration properties. */
  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String path,
      UCCredentialHadoopConfs.PathOperation pathOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new PathCredId(credContextId(catalogUri, scheme, tokenProvider), path, pathOp.value()));
  }

  /**
   * Fetches temporary credentials for {@code credId} and builds the cloud-provider specific Hadoop
   * configuration properties for {@code scheme}. Shared by all {@code create*CredProps} entry
   * points, which differ only in how they construct the {@link CredId}.
   */
  private static Map<String, String> createCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    Optional<CloudType> cloudType = CloudType.fromScheme(scheme);
    if (cloudType.isEmpty()) {
      // Unsupported scheme: skip the credential fetch entirely and return no props.
      return Collections.emptyMap();
    }
    GenericCredential cred =
        fetchGenericCredential(
            hadoopConf, apiClient, catalogUri, tokenProvider, appVersions, credId);
    return CredPropsBuilder.forCloud(cloudType.get())
        .applyBaseConf(hadoopConf)
        .applyImplOverrides(credScopedFsEnabled, hadoopConf)
        .applyRenewalContext(renewCredEnabled, catalogUri, tokenProvider, credId, appVersions)
        .writeCredKeys(renewCredEnabled, cred)
        .build();
  }

  /**
   * Derives a stable id for the credential context so caches only reuse a vended credential across
   * requests that would receive the same one. A vended credential is a function of the catalog
   * endpoint, storage scheme, and auth identity, so all three are folded into the hash alongside
   * the auth configs from {@code tokenProvider}.
   */
  static String credContextId(String catalogUri, String scheme, TokenProvider tokenProvider) {
    Objects.requireNonNull(catalogUri, "catalogUri is required");
    Objects.requireNonNull(scheme, "scheme is required");
    Objects.requireNonNull(tokenProvider, "tokenProvider is required");

    Map<String, String> context = new HashMap<>(tokenProvider.configs());
    context.put(CRED_CONTEXT_CATALOG_URI_KEY, catalogUri);
    context.put(CRED_CONTEXT_SCHEME_KEY, scheme);
    return MapIdGenerator.generateId(context);
  }

  private static GenericCredential fetchGenericCredential(
      Configuration hadoopConf,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    boolean credCacheEnabled =
        hadoopConf.getBoolean(
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);
    if (!credCacheEnabled) {
      return createCredential(apiClient, catalogUri, tokenProvider, appVersions, credId);
    }

    long renewalLeadTimeMillis =
        hadoopConf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    return initialCredCache.access(
        credId,
        () ->
            new RenewableCredential(
                renewalLeadTimeMillis,
                ClockUtil.resolveClock(hadoopConf),
                createCredential(apiClient, catalogUri, tokenProvider, appVersions, credId)));
  }

  private static GenericCredential createCredential(
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    ApiClient client =
        apiClient != null ? apiClient : createApiClient(catalogUri, tokenProvider, appVersions);
    return genericCredFetcherFactory.create(client, credId).createCredential();
  }

  private static ApiClient createApiClient(
      String catalogUri, TokenProvider tokenProvider, Map<String, String> appVersions) {
    return ApiClientUtils.create(
        URI.create(catalogUri),
        tokenProvider,
        UCHadoopConfConstants.createRequestRetryPolicy(null),
        appVersions);
  }
}
