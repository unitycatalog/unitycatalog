package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.spark.auth.AuthConfigUtils;
import io.unitycatalog.spark.auth.CredPropsUtil;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.base.Preconditions;

/** A Spark catalog plugin to get/manage tables in Unity Catalog. */
public class UCSingleCatalog implements TableCatalog, SupportsNamespaces {
  private static final Logger LOG = LoggerFactory.getLogger(UCSingleCatalog.class);

  static final ThreadLocal<Boolean> LOAD_DELTA_CATALOG = ThreadLocal.withInitial(() -> true);
  static final ThreadLocal<Boolean> DELTA_CATALOG_LOADED = ThreadLocal.withInitial(() -> false);

  private URI uri = null;
  private TokenProvider tokenProvider = null;
  private boolean renewCredEnabled = false;
  private SchemasApi schemasApi = null;
  private TablesApi tablesApi = null;
  private TemporaryCredentialsApi tempCredApi = null;

  private volatile TableCatalog delegate = null;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    String urlStr = options.get(OptionsUtil.URI);
    Preconditions.checkArgument(
        urlStr != null, "uri must be specified for Unity Catalog '%s'", name);
    try {
      uri = new URI(urlStr);
    } catch (Exception e) {
      throw new RuntimeException("Invalid URI: " + urlStr, e);
    }
    tokenProvider = TokenProvider.create(AuthConfigUtils.buildAuthConfigs(options));
    renewCredEnabled =
        OptionsUtil.getBoolean(
            options,
            OptionsUtil.RENEW_CREDENTIAL_ENABLED,
            OptionsUtil.DEFAULT_RENEW_CREDENTIAL_ENABLED);

    // Initialize the ApiClient.
    ApiClient apiClient =
        ApiClientFactory.createApiClient(
            JitterDelayRetryPolicy.builder().build(), uri, tokenProvider);

    // Initialize the UCProxy.
    tempCredApi = new TemporaryCredentialsApi(apiClient);
    schemasApi = new SchemasApi(apiClient);
    tablesApi = new TablesApi(apiClient);
    UCProxy proxy =
        new UCProxy(uri, tokenProvider, renewCredEnabled, schemasApi, tablesApi, tempCredApi);
    proxy.initialize(name, options);

    // Initialize the delegate catalog.
    if (LOAD_DELTA_CATALOG.get()) {
      try {
        delegate =
            (TableCatalog)
                Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .getDeclaredConstructor()
                    .newInstance();
        ((DelegatingCatalogExtension) delegate).setDelegateCatalog(proxy);
        delegate.initialize(name, options);
        DELTA_CATALOG_LOADED.set(true);
      } catch (ClassNotFoundException e) {
        LOG.warn("DeltaCatalog is not available in the classpath", e);
        delegate = proxy;
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize DeltaCatalog", e);
      }
    } else {
      delegate = proxy;
    }
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return delegate.listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return delegate.loadTable(ident);
  }

  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return delegate.loadTable(ident, version);
  }

  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return delegate.loadTable(ident, timestamp);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return delegate.tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties) {
    checkUnsupportedNestedNamespace(ident.namespace());
    boolean hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL);
    boolean hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION);
    if (hasExternalClause && !hasLocationClause) {
      throw new RuntimeException("Cannot create EXTERNAL TABLE without location.");
    }
    boolean isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute();

    // If both EXTERNAL and LOCATION are not specified in the CREATE TABLE command, and the table is
    // not a path table like parquet.`/file/path`, we generate the UC-managed table location here.
    if (!hasExternalClause && !hasLocationClause && !isPathTable) {
      // Check that caller shouldn't set some properties
      List<String> restrictedProps =
          Arrays.asList(
              UCTableProperties.UC_TABLE_ID_KEY,
              UCTableProperties.UC_TABLE_ID_KEY_OLD,
              TableCatalog.PROP_IS_MANAGED_LOCATION);
      for (String prop : restrictedProps) {
        if (properties.containsKey(prop)) {
          throw new RuntimeException("Cannot specify property '" + prop + "'.");
        }
      }
      // Setting the catalogManaged table feature is required for creating a managed table.
      if (!properties.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY)
          && !properties.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)) {
        throw new RuntimeException(
            "Managed table creation requires table property '"
                + UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW
                + "'='"
                + UCTableProperties.DELTA_CATALOG_MANAGED_VALUE
                + "' to be set.");
      }
      // Caller should not set these two table properties to values other than "supported". This is
      // the only documented value.
      List<String> catalogManagedKeys =
          Arrays.asList(
              UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
              UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW);
      for (String k : catalogManagedKeys) {
        String v = properties.get(k);
        if (v != null && !v.equals(UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)) {
          throw new RuntimeException("Invalid property value '" + v + "' for '" + k + "'.");
        }
      }

      // Get staging table location and table id from UC
      CreateStagingTable createStagingTable =
          new CreateStagingTable()
              .catalogName(name())
              .schemaName(ident.namespace()[0])
              .name(ident.name());
      StagingTableInfo stagingTableInfo;
      try {
        stagingTableInfo = tablesApi.createStagingTable(createStagingTable);
      } catch (ApiException e) {
        throw new RuntimeException("Failed to create staging table", e);
      }
      String stagingLocation = stagingTableInfo.getStagingLocation();
      String stagingTableId = stagingTableInfo.getId();

      Map<String, String> newProps = new HashMap<>(properties);
      newProps.put(TableCatalog.PROP_LOCATION, stagingTableInfo.getStagingLocation());
      // Sets both the new and old table ID property while it's being renamed.
      newProps.put(UCTableProperties.UC_TABLE_ID_KEY, stagingTableInfo.getId());
      newProps.put(UCTableProperties.UC_TABLE_ID_KEY_OLD, stagingTableInfo.getId());
      // `PROP_IS_MANAGED_LOCATION` is used to indicate that the table location is not
      // user-specified but system-generated, which is exactly the case here.
      newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");

      TemporaryCredentials temporaryCredentials;
      try {
        temporaryCredentials =
            tempCredApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(stagingTableId)
                    .operation(TableOperation.READ_WRITE));
      } catch (ApiException e) {
        throw new RuntimeException("Failed to generate temporary credentials", e);
      }
      Map<String, String> credentialProps =
          CredPropsUtil.createTableCredProps(
              renewCredEnabled,
              CatalogUtils.stringToURI(stagingLocation).getScheme(),
              uri.toString(),
              tokenProvider,
              stagingTableId,
              TableOperation.READ_WRITE,
              temporaryCredentials);
      setCredentialProps(newProps, credentialProps);

      try {
        return delegate.createTable(ident, columns, partitions, newProps);
      } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
          | NoSuchNamespaceException e) {
        throw new RuntimeException("Failed to create table", e);
      }
    } else if (hasLocationClause) {
      String location = properties.get(TableCatalog.PROP_LOCATION);
      assert location != null;
      TemporaryCredentials cred;
      try {
        cred =
            tempCredApi.generateTemporaryPathCredentials(
                new GenerateTemporaryPathCredential()
                    .url(location)
                    .operation(PathOperation.PATH_CREATE_TABLE));
      } catch (ApiException e) {
        throw new RuntimeException("Failed to generate temporary path credentials", e);
      }
      Map<String, String> newProps = new HashMap<>(properties);

      Map<String, String> credentialProps =
          CredPropsUtil.createPathCredProps(
              renewCredEnabled,
              CatalogUtils.stringToURI(location).getScheme(),
              uri.toString(),
              tokenProvider,
              location,
              PathOperation.PATH_CREATE_TABLE,
              cred);

      setCredentialProps(newProps, credentialProps);
      try {
        return delegate.createTable(ident, columns, partitions, newProps);
      } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
          | NoSuchNamespaceException e) {
        throw new RuntimeException("Failed to create table", e);
      }
    } else {
      // TODO: for path-based tables, Spark should generate a location property using the qualified
      //       path string.
      try {
        return delegate.createTable(ident, columns, partitions, properties);
      } catch (org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
          | NoSuchNamespaceException e) {
        throw new RuntimeException("Failed to create table", e);
      }
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    throw new UnsupportedOperationException("deprecated `createTable` should not be called");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("Altering a table is not supported yet");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return delegate.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("Renaming a table is not supported yet");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return ((DelegatingCatalogExtension) delegate).loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    try {
      ((DelegatingCatalogExtension) delegate).createNamespace(namespace, metadata);
    } catch (org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException e) {
      throw new RuntimeException("Namespace already exists", e);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    ((DelegatingCatalogExtension) delegate).alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    try {
      return ((DelegatingCatalogExtension) delegate).dropNamespace(namespace, cascade);
    } catch (org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException e) {
      throw new RuntimeException("Namespace is not empty", e);
    }
  }

  // Static methods from companion object

  static void setCredentialProps(Map<String, String> props, Map<String, String> credentialProps) {
    props.putAll(credentialProps);
    // TODO: Delta requires the options to be set twice in the properties, with and without the
    //       `option.` prefix. We should revisit this in Delta.
    credentialProps.forEach((k, v) -> props.put(TableCatalog.OPTION_PREFIX + k, v));
  }

  static void checkUnsupportedNestedNamespace(String[] namespace) {
    if (namespace.length > 1) {
      throw new RuntimeException(
          "Nested namespaces are not supported: " + String.join(".", namespace));
    }
  }

  /**
   * Constructs a fully qualified table name for Unity Catalog API calls.
   *
   * <p>This method creates a three-part name in the format `catalog.schema.table` by combining the
   * catalog name with the schema name (from the identifier's namespace) and table name. It is NOT
   * backtick quoted like what is usually used in SQL statements even if the names have special
   * characters like hyphens.
   *
   * <p>Example: catalogName=catalog, ident=(schema, table): it returns "catalog.schema.table"
   * catalogName=cata-log, ident=(sche-ma, ta-ble): returns "cata-log.sche-ma.ta-ble"
   * catalogName=catalog, ident=((schema1, schema2), table): throws ApiException(Nested namespace
   * not supported)
   *
   * @param catalogName the name of the catalog
   * @param ident the table identifier containing the namespace (schema) and table name
   * @return a fully qualified table name in the format "catalog.schema.table"
   * @throws ApiException if the identifier contains nested namespaces
   */
  static String fullTableNameForApi(String catalogName, Identifier ident) {
    checkUnsupportedNestedNamespace(ident.namespace());
    return catalogName + "." + ident.namespace()[0] + "." + ident.name();
  }
}
