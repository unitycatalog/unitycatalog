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
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

  static final CredentialCache initialCredCache = CredentialCache.createInitialCredentialCache();

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
  private static final String GCS_CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String ABFS_FIXED_SAS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  // Keys used to build the credential-context id (see #credContextId).
  private static final String CRED_CONTEXT_CATALOG_URI_KEY = "catalogUri";
  private static final String CRED_CONTEXT_SCHEME_KEY = "scheme";

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final HashMap<String, String> builder = new HashMap<>();

    public T set(String key, String value) {
      builder.put(key, value);
      return self();
    }

    public T uri(String uri) {
      builder.put(UCHadoopConfConstants.UC_URI_KEY, uri);
      return self();
    }

    public T tokenProvider(TokenProvider tokenProvider) {
      // As we can only propagate the properties with prefix 'fs.*' to the FileSystem
      // implementation. So let's add the prefix here.
      tokenProvider
          .configs()
          .forEach((key, value) -> builder.put(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
      return self();
    }

    /** Applies the credential-scope identity properties carried by {@code credId}. */
    public T credId(CredId credId) {
      credId.props().forEach(this::set);
      return self();
    }

    public T appVersions(Map<String, String> appVersions) {
      appVersions.forEach(
          (k, v) -> builder.put(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + k, v));
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

  static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

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
      // The upstream GCS connector defaults this to true which causes the connector to
      // stat every ancestor directory on file creation. With UC-vended downscoped tokens
      // (scoped to a table's path prefix) these ancestor stats return 403. Default to
      // false; users with broader credentials can opt back in via Hadoop/Spark config.
      set(GCS_CONFLICT_CHECK_KEY, hadoopConf.get(GCS_CONFLICT_CHECK_KEY, "false"));
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
      set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
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
      boolean credScopedFsEnabled, Configuration hadoopConf, GenericCredential cred) {
    return new S3PropsBuilder(credScopedFsEnabled, hadoopConf)
        .set("fs.s3a.access.key", cred.awsAccessKeyId())
        .set("fs.s3a.secret.key", cred.awsSecretAccessKey())
        .set("fs.s3a.session.token", cred.awsSessionToken())
        .build();
  }

  private static S3PropsBuilder s3TempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      GenericCredential cred) {
    S3PropsBuilder builder =
        new S3PropsBuilder(credScopedFsEnabled, hadoopConf)
            .set(UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .set(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, cred.awsAccessKeyId())
            .set(UCHadoopConfConstants.S3A_INIT_SECRET_KEY, cred.awsSecretAccessKey())
            .set(UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, cred.awsSessionToken());

    // For the static credential case, nullable expiration time is possible.
    if (cred.expirationTimeMillis() != null) {
      builder.set(
          UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
          String.valueOf(cred.expirationTimeMillis()));
    }

    return builder;
  }

  private static Map<String, String> gsFixedCredProps(
      boolean credScopedFsEnabled, Configuration hadoopConf, GenericCredential cred) {
    Long expirationTime =
        cred.expirationTimeMillis() == null ? Long.MAX_VALUE : cred.expirationTimeMillis();
    return new GcsPropsBuilder(credScopedFsEnabled, hadoopConf)
        .set(GCS_ACCESS_TOKEN_KEY, cred.gcsOauthToken())
        .set(GCS_ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(expirationTime))
        .build();
  }

  private static GcsPropsBuilder gcsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      GenericCredential cred) {
    GcsPropsBuilder builder =
        new GcsPropsBuilder(credScopedFsEnabled, hadoopConf)
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GCS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .set(UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, cred.gcsOauthToken());

    // For the static credential case, nullable expiration time is possible.
    if (cred.expirationTimeMillis() != null) {
      builder.set(
          UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
          String.valueOf(cred.expirationTimeMillis()));
    }

    return builder;
  }

  private static Map<String, String> abfsFixedCredProps(
      boolean credScopedFsEnabled, Configuration hadoopConf, GenericCredential cred) {
    return new AbfsPropsBuilder(credScopedFsEnabled, hadoopConf)
        .set(ABFS_FIXED_SAS_TOKEN_KEY, cred.azureSasToken())
        .build();
  }

  private static AbfsPropsBuilder abfsTempCredPropsBuilder(
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String uri,
      TokenProvider tokenProvider,
      GenericCredential cred) {
    AbfsPropsBuilder builder =
        new AbfsPropsBuilder(credScopedFsEnabled, hadoopConf)
            .set(
                UCHadoopConfConstants.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
                ABFS_VENDED_TOKEN_PROVIDER_CLASS)
            .uri(uri)
            .tokenProvider(tokenProvider)
            .set(UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, cred.azureSasToken());

    // For the static credential case, nullable expiration time is possible.
    if (cred.expirationTimeMillis() != null) {
      builder.set(
          UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
          String.valueOf(cred.expirationTimeMillis()));
    }

    return builder;
  }

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
    if (!CloudType.isSupportedScheme(scheme)) {
      return Collections.emptyMap();
    }
    GenericCredential cred =
        fetchGenericCredential(
            hadoopConf, apiClient, catalogUri, tokenProvider, appVersions, credId);
    switch (scheme) {
      case "s3":
        if (renewCredEnabled) {
          return s3TempCredPropsBuilder(
                  credScopedFsEnabled, hadoopConf, catalogUri, tokenProvider, cred)
              .credId(credId)
              .appVersions(appVersions)
              .build();
        } else {
          return s3FixedCredProps(credScopedFsEnabled, hadoopConf, cred);
        }
      case "gs":
        if (renewCredEnabled) {
          return gcsTempCredPropsBuilder(
                  credScopedFsEnabled, hadoopConf, catalogUri, tokenProvider, cred)
              .credId(credId)
              .appVersions(appVersions)
              .build();
        } else {
          return gsFixedCredProps(credScopedFsEnabled, hadoopConf, cred);
        }
      case "abfss":
      case "abfs":
        if (renewCredEnabled) {
          return abfsTempCredPropsBuilder(
                  credScopedFsEnabled, hadoopConf, catalogUri, tokenProvider, cred)
              .credId(credId)
              .appVersions(appVersions)
              .build();
        } else {
          return abfsFixedCredProps(credScopedFsEnabled, hadoopConf, cred);
        }
      default:
        return Collections.emptyMap();
    }
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
