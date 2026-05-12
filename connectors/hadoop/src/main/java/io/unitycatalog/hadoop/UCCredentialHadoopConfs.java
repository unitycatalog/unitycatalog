package io.unitycatalog.hadoop;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
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
 *     .initialCredentials(creds)
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
    private TemporaryCredentials initialCredentials;
    private StorageCredential initialStorageCredential;
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

    /**
     * The token provider for UC authentication. Required by default (since credential renewal is
     * enabled by default); may be {@code null} only when credential renewal is explicitly disabled.
     */
    public Builder tokenProvider(TokenProvider tokenProvider) {
      this.tokenProvider = tokenProvider;
      return this;
    }

    /**
     * (Required) The initial temporary credentials vended by UC (AWS session credentials, GCP OAuth
     * token, or Azure SAS). Typically, allocated once by the job driver and propagated to all
     * worker nodes so that each worker reuses the same credential rather than vending a new one.
     *
     * <p>Mutually exclusive with {@link #initialStorageCredential(StorageCredential)}.
     */
    public Builder initialCredentials(TemporaryCredentials initialCredentials) {
      Preconditions.checkState(
          initialStorageCredential == null,
          "initialStorageCredential is already set; cannot also set initialCredentials. The two"
              + " select different credentials APIs (UC vs UC Delta).");
      this.initialCredentials = initialCredentials;
      return this;
    }

    /**
     * (Required for UC Delta) The initial UC Delta storage credential returned by the UC Delta
     * table API.
     *
     * <p>Mutually exclusive with {@link #initialCredentials(TemporaryCredentials)}.
     */
    public Builder initialStorageCredential(StorageCredential initialStorageCredential) {
      Preconditions.checkState(
          initialCredentials == null,
          "initialCredentials is already set; cannot also set initialStorageCredential. The two"
              + " select different credentials APIs (UC vs UC Delta).");
      this.initialStorageCredential = initialStorageCredential;
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
     * Builds Hadoop properties for a <em>table's</em> storage location.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if a required field is missing
     */
    public Map<String, String> buildForTable(String tableId, TableOperation tableOperation) {
      validateUcApi();
      return withEngineVersionProps(
          CredPropsUtil.createTableCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              tableId,
              tableOperation,
              initialCredentials));
    }

    /**
     * Builds Hadoop properties for a UC Delta table's storage location.
     *
     * @param catalog the UC catalog name
     * @param schema the UC schema name
     * @param table the UC table name
     * @param operation the credential operation to request from the UC Delta credentials API
     * @param location the table storage location used to select a returned storage credential
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalArgumentException if a table identity, operation, or location field is missing
     * @throws IllegalStateException if a required builder field is missing
     */
    public Map<String, String> buildForTable(
        String catalog,
        String schema,
        String table,
        CredentialOperation operation,
        String location) {
      UCDeltaTableIdentifier identifier = UCDeltaTableIdentifier.of(catalog, schema, table);
      Preconditions.checkArgument(operation != null, "operation is required");
      Preconditions.checkArgument(location != null && !location.isEmpty(), "location is required");
      validateUcDeltaApi();
      TemporaryCredentials tempCreds =
          DeltaStorageCredentialUtil.toTemporaryCredentials(initialStorageCredential);
      return withEngineVersionProps(
          CredPropsUtil.createDeltaTableCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              identifier,
              location,
              operation,
              tempCreds));
    }

    /**
     * Builds Hadoop properties for an <em>external path</em>.
     *
     * @return unmodifiable map; empty if the scheme is unrecognized
     * @throws IllegalStateException if a required field is missing
     */
    public Map<String, String> buildForPath(String path, PathOperation pathOperation) {
      validateUcApi();
      return withEngineVersionProps(
          CredPropsUtil.createPathCredProps(
              credentialRenewalEnabled,
              credentialScopedFsEnabled,
              hadoopConf,
              scheme,
              catalogUri,
              tokenProvider,
              path,
              pathOperation,
              initialCredentials));
    }

    private Map<String, String> withEngineVersionProps(Map<String, String> props) {
      if (props.isEmpty() || engineVersionProps.isEmpty()) {
        return props;
      }
      Map<String, String> merged = new HashMap<>(props);
      merged.putAll(engineVersionProps);
      return Collections.unmodifiableMap(merged);
    }

    private void validateCommon() {
      Preconditions.checkState(
          !credentialRenewalEnabled || tokenProvider != null,
          "tokenProvider is required when credential renewal is enabled");
    }

    private void validateUcApi() {
      validateCommon();
      Preconditions.checkState(
          initialCredentials != null, "initialCredentials is required for the UC credentials API");
    }

    private void validateUcDeltaApi() {
      validateCommon();
      Preconditions.checkState(
          initialStorageCredential != null,
          "initialStorageCredential is required for the UC Delta credentials API");
    }
  }
}
