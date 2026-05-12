package io.unitycatalog.hadoop;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Produces the Hadoop configuration properties that a connector (Spark, Flink, Trino, etc.) must
 * inject so that cloud storage can be accessed with Unity-Catalog-vended credentials.
 *
 * <pre>{@code
 * Map<String, String> props = UCCredentialHadoopConfs.builder(uri, "s3")
 *     .tokenProvider(tokenProvider)
 *     .enableCredentialRenewal(true)
 *     .enableCredentialScopedFs(true)
 *     .hadoopConf(hadoopConf)
 *     .buildForTable(tableId, TableOperation.READ_WRITE);
 * }</pre>
 *
 * @since 0.5.0
 */
public final class UCCredentialHadoopConfs {

  private UCCredentialHadoopConfs() {}

  /**
   * Creates a new {@link Builder} with the two required fields.
   *
   * @param catalogUri the Unity Catalog server base URI, e.g. {@code "https://my-uc-server"}
   * @param scheme the storage URI scheme ({@code "s3"}, {@code "gs"}, {@code "abfs"}, or {@code
   *     "abfss"})
   */
  public static Builder builder(String catalogUri, String scheme) {
    return new Builder(catalogUri, scheme);
  }

  /**
   * Collects credential settings and produces Hadoop configuration properties via {@link
   * #buildForTable} or {@link #buildForPath}.
   */
  public static final class Builder {

    private final String catalogUri;
    private final String scheme;

    private TokenProvider tokenProvider;
    private boolean credentialRenewalEnabled = true;
    private boolean credentialScopedFsEnabled = true;
    private Configuration hadoopConf = new Configuration(false);
    private final Map<String, String> engineVersionProps = new LinkedHashMap<>();

    private Builder(String catalogUri, String scheme) {
      Preconditions.checkArgument(catalogUri != null, "catalogUri is required");
      Preconditions.checkArgument(scheme != null, "scheme is required");
      this.catalogUri = catalogUri;
      this.scheme = scheme;
    }

    /** The token provider for UC authentication. Required for all credential operations. */
    public Builder tokenProvider(TokenProvider tokenProvider) {
      this.tokenProvider = tokenProvider;
      return this;
    }

    /**
     * Whether to enable automatic credential renewal (default {@code true}). When enabled,
     * configures a vended-token provider that refreshes credentials before they expire.
     */
    public Builder enableCredentialRenewal(boolean enabled) {
      this.credentialRenewalEnabled = enabled;
      return this;
    }

    /**
     * Whether to enable credential-scoped filesystem caching (default {@code true}). When enabled,
     * overrides {@code fs.<scheme>.impl} so that filesystem instances are reused per credential
     * scope.
     */
    public Builder enableCredentialScopedFs(boolean enabled) {
      this.credentialScopedFsEnabled = enabled;
      return this;
    }

    /**
     * The engine's existing Hadoop {@link Configuration}. When credential-scoped FS is enabled, the
     * builder extracts the relevant {@code fs.<scheme>.impl} values so it can preserve the original
     * filesystem implementation before overriding it. Engine connectors should pass their Hadoop
     * config here; they do not need to know which specific keys are used internally.
     */
    public Builder hadoopConf(Configuration conf) {
      this.hadoopConf = conf;
      return this;
    }

    /**
     * Records engine versions (e.g. {@code Map.of("Spark", "4.0.0")}) to be propagated to the
     * User-Agent header on UC API calls so the server can trace which engine versions are calling.
     * Engines should typically register their own version plus any relevant runtime versions
     * (Delta, Java, Scala, etc.).
     */
    public Builder addEngineVersions(Map<String, String> versions) {
      Preconditions.checkNotNull(versions, "engine versions required");
      versions.forEach(this::addEngineVersion);
      return this;
    }

    private void addEngineVersion(String name, String version) {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "engine version name required");
      Preconditions.checkArgument(
          version != null && !version.isEmpty(), "engine version value for '%s' required", name);
      engineVersionProps.put(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + name, version);
    }

    /**
     * Builds Hadoop properties for a <em>table's</em> storage location using the UC REST
     * credentials API.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if {@code tokenProvider} is not set
     * @throws ApiException if the credential fetch from the UC REST API fails
     */
    public Map<String, String> buildForTable(String tableId, TableOperation tableOperation)
        throws ApiException {
      if (!isRecognizedScheme()) {
        return Collections.emptyMap();
      }
      Preconditions.checkState(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.mergeEngineVersionProps(
          CredPropsUtil.fetchTableCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              tableId,
              tableOperation,
              appVersions()),
          engineVersionProps);
    }

    /**
     * Builds Hadoop properties for a UC Delta table's storage location using the UC Delta
     * credentials API.
     *
     * @param catalog the UC catalog name
     * @param schema the UC schema name
     * @param table the UC table name
     * @param operation the credential operation to request from the UC Delta credentials API
     * @param location the table storage location used to select a returned storage credential
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalArgumentException if a table identity, operation, or location field is missing
     * @throws IllegalStateException if {@code tokenProvider} is not set
     * @throws ApiException if the credential fetch from the UC Delta API fails
     */
    public Map<String, String> buildForTable(
        String catalog, String schema, String table, TableOperation operation, String location)
        throws ApiException {
      UCDeltaTableIdentifier identifier = UCDeltaTableIdentifier.of(catalog, schema, table);
      Preconditions.checkArgument(operation != null, "operation is required");
      Preconditions.checkArgument(location != null && !location.isEmpty(), "location is required");
      if (!isRecognizedScheme()) {
        return Collections.emptyMap();
      }
      Preconditions.checkState(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.mergeEngineVersionProps(
          CredPropsUtil.fetchDeltaTableCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              identifier,
              location,
              operation,
              appVersions()),
          engineVersionProps);
    }

    /**
     * Builds Hadoop properties for an <em>external path</em> using the UC REST credentials API.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if {@code tokenProvider} is not set
     * @throws ApiException if the credential fetch from the UC REST API fails
     */
    public Map<String, String> buildForPath(String path, PathOperation pathOperation)
        throws ApiException {
      if (!isRecognizedScheme()) {
        return Collections.emptyMap();
      }
      Preconditions.checkState(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.mergeEngineVersionProps(
          CredPropsUtil.fetchPathCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              path,
              pathOperation,
              appVersions()),
          engineVersionProps);
    }

    private Map<String, String> appVersions() {
      Map<String, String> result = new HashMap<>();
      String prefix = UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX;
      engineVersionProps.forEach(
          (k, v) -> {
            if (k.startsWith(prefix)) result.put(k.substring(prefix.length()), v);
          });
      return result;
    }

    private boolean isRecognizedScheme() {
      return "s3".equals(scheme)
          || "gs".equals(scheme)
          || "abfs".equals(scheme)
          || "abfss".equals(scheme);
    }
  }
}
