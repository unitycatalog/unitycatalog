package io.unitycatalog.hadoop;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
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
   * The access operation requested for a Unity Catalog table credential.
   *
   * @since 0.5.0
   */
  public enum TableOperation {
    READ,
    READ_WRITE;

    public String value() {
      return name();
    }
  }

  /**
   * The access operation requested for a Unity Catalog external path credential.
   *
   * @since 0.5.0
   */
  public enum PathOperation {
    PATH_READ,
    PATH_READ_WRITE,
    PATH_CREATE_TABLE;

    public String value() {
      return name();
    }
  }

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
    private ApiClient apiClient;
    private boolean credentialRenewalEnabled = true;
    private boolean credentialScopedFsEnabled = true;

    private Configuration hadoopConf = new Configuration(false);
    private final Map<String, String> appVersions = new LinkedHashMap<>();

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
     * An existing {@link ApiClient} to reuse for the initial credential fetch. When set, no new
     * HTTP connection pool is created. When absent, one is created from {@code catalogUri} and
     * {@code tokenProvider}.
     */
    public Builder apiClient(ApiClient apiClient) {
      this.apiClient = apiClient;
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
     * Records app versions (e.g. {@code Map.of("Spark", "4.0.0")}) to be propagated to the
     * User-Agent header on UC API calls so the server can trace which app versions are calling.
     * Callers should typically register the engine version plus any relevant runtime versions
     * (Delta, Java, Scala, etc.).
     */
    public Builder addAppVersions(Map<String, String> versions) {
      Preconditions.checkNotNull(versions, "app versions required");
      versions.forEach(this::addAppVersion);
      return this;
    }

    private void addAppVersion(String name, String version) {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "app version name required");
      Preconditions.checkArgument(
          version != null && !version.isEmpty(), "app version value for '%s' required", name);
      appVersions.put(name, version);
    }

    /**
     * Builds Hadoop properties for a table using the UC REST credentials API.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalArgumentException if {@code tokenProvider} is not set
     * @throws ApiException if the credential fetch from the UC REST API fails
     */
    public Map<String, String> buildForTable(String tableId, TableOperation tableOperation)
        throws ApiException {
      Preconditions.checkArgument(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.fetchTableCredProps(
          credentialRenewalEnabled,
          credentialScopedFsEnabled,
          hadoopConf,
          scheme,
          apiClient,
          catalogUri,
          tokenProvider,
          tableId,
          tableOperation,
          appVersions);
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
     * @throws IllegalArgumentException if a table identity, operation, location field, or {@code
     *     tokenProvider} is missing
     * @throws ApiException if the credential fetch from the UC Delta API fails
     */
    public Map<String, String> buildForTable(
        String catalog, String schema, String table, TableOperation operation, String location)
        throws ApiException {
      UCDeltaTableIdentifier identifier = UCDeltaTableIdentifier.of(catalog, schema, table);
      Preconditions.checkArgument(operation != null, "operation is required");
      Preconditions.checkArgument(location != null && !location.isEmpty(), "location is required");
      Preconditions.checkArgument(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.fetchDeltaTableCredProps(
          credentialRenewalEnabled,
          credentialScopedFsEnabled,
          hadoopConf,
          scheme,
          apiClient,
          catalogUri,
          tokenProvider,
          identifier,
          location,
          operation,
          appVersions);
    }

    /**
     * Builds Hadoop properties for an <em>external path</em> using the UC REST credentials API.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalArgumentException if {@code tokenProvider} is not set
     * @throws ApiException if the credential fetch from the UC REST API fails
     */
    public Map<String, String> buildForPath(String path, PathOperation pathOperation)
        throws ApiException {
      Preconditions.checkArgument(tokenProvider != null, "tokenProvider is required");
      return CredPropsUtil.fetchPathCredProps(
          credentialRenewalEnabled,
          credentialScopedFsEnabled,
          hadoopConf,
          scheme,
          apiClient,
          catalogUri,
          tokenProvider,
          path,
          pathOperation,
          appVersions);
    }
  }
}
