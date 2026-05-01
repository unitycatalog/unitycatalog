package io.unitycatalog.spark

import java.net.URI
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{
  Dependency => UCDependency,
  DependencyList => UCDependencyList,
  TableDependency => UCTableDependency,
  TableInfo => UCTableInfo,
  _
}
import io.unitycatalog.client.retry.JitterDelayRetryPolicy
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.hadoop.UCCredentialHadoopConfs
import io.unitycatalog.spark.auth.AuthConfigUtils
import io.unitycatalog.spark.utils.OptionsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException, ViewAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.json4s.{JBool, JObject, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.sparkproject.guava.base.Preconditions

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog
  extends StagingTableCatalog
  with SupportsNamespaces
  with TableViewCatalog
  with Logging {

  private[this] var uri: URI = null
  private[this] var tokenProvider: TokenProvider = null
  private[this] var renewCredEnabled: Boolean = false
  private[this] var credScopedFsEnabled: Boolean = false
  private[this] var apiClient: ApiClient = null;
  private[this] var temporaryCredentialsApi: TemporaryCredentialsApi = null
  private[this] var tablesApi: TablesApi = null

  // The connector keeps two references to the catalog underneath:
  //
  //   - `delegate` is the *outermost* `TableCatalog` the connector exposes for table
  //     operations. When DeltaCatalog is on the classpath it wraps `ucProxy` via
  //     `DelegatingCatalogExtension`, so Delta can intercept managed / external Delta-table
  //     create / load paths and add Delta-specific behavior. When DeltaCatalog is not on the
  //     classpath, `delegate` is set to `ucProxy` itself so non-Delta table paths still work.
  //
  //   - `ucProxy` is the underlying UC REST client wrapped as a Spark `TableViewCatalog`. It
  //     is always the real `UCProxy` instance so we can route view operations directly. Delta
  //     has no role in view handling, so view-side methods (`createView` / `loadView` /
  //     `dropView` / `listViews`) bypass `delegate` and call `ucProxy` directly, avoiding the
  //     need to unwrap the `DelegatingCatalogExtension` chain.
  @volatile private var delegate: TableCatalog = null
  @volatile private var ucProxy: UCProxy = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val urlStr = options.get(OptionsUtil.URI)
    Preconditions.checkArgument(urlStr != null,
      "uri must be specified for Unity Catalog '%s'", name)
    uri = new URI(urlStr)
    tokenProvider = TokenProvider.create(AuthConfigUtils.buildAuthConfigs(options));
    renewCredEnabled = OptionsUtil.getBoolean(options,
      OptionsUtil.RENEW_CREDENTIAL_ENABLED,
      OptionsUtil.DEFAULT_RENEW_CREDENTIAL_ENABLED)
    credScopedFsEnabled = OptionsUtil.getBoolean(options,
      OptionsUtil.CRED_SCOPED_FS_ENABLED,
      OptionsUtil.DEFAULT_CRED_SCOPED_FS_ENABLED)
    val serverSidePlanningEnabled = OptionsUtil.getBoolean(options,
      OptionsUtil.SERVER_SIDE_PLANNING_ENABLED,
      OptionsUtil.DEFAULT_SERVER_SIDE_PLANNING_ENABLED)

    apiClient = ApiClientFactory.createApiClient(
      JitterDelayRetryPolicy.builder().build(),uri, tokenProvider)
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient)
    tablesApi = new TablesApi(apiClient)
    val proxy = new UCProxy(uri, tokenProvider, renewCredEnabled, credScopedFsEnabled,
      serverSidePlanningEnabled, apiClient, tablesApi, temporaryCredentialsApi)
    proxy.initialize(name, options)
    ucProxy = proxy
    if (UCSingleCatalog.LOAD_DELTA_CATALOG.get()) {
      try {
        delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
        delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
        delegate.initialize(name, options)
        UCSingleCatalog.DELTA_CATALOG_LOADED.set(true)
      } catch {
        case e: ClassNotFoundException =>
          logWarning("DeltaCatalog is not available in the classpath", e)
          delegate = proxy
      }
    } else {
      delegate = proxy
    }
  }

  override def name(): String = delegate.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = delegate.listTables(namespace)

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident)

  override def loadTable(ident: Identifier, version:  String): Table = delegate.loadTable(ident, version)

  override def loadTable(ident: Identifier, timestamp:  Long): Table = delegate.loadTable(ident, timestamp)

  override def tableExists(ident: Identifier): Boolean = {
    delegate.tableExists(ident)
  }

  override def listViews(namespace: Array[String]): Array[Identifier] =
    ucProxy.listViews(namespace)

  override def loadView(ident: Identifier): ViewInfo =
    ucProxy.loadView(ident)

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo =
    ucProxy.createView(ident, info)

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo =
    throw new UnsupportedOperationException("Replacing a view is not supported yet")

  override def dropView(ident: Identifier): Boolean =
    ucProxy.dropView(ident)

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    ucProxy.renameView(oldIdent, newIdent)

  /**
   * `TableViewCatalog.loadTableOrView` is Spark's single-RPC perf entry point: the resolver
   * calls it once per identifier and uses its return type to discriminate tables from views,
   * instead of falling back from `loadTable` to `loadView`. We honor that contract by
   * performing one UC `getTable` and dispatching on the UC `TableType`:
   *
   *   - If `UCSingleCatalog.isViewLikeTableType` accepts the row, we build a `ViewInfo` and
   *     return it wrapped in a `MetadataTable`. Spark recognizes this shape and routes
   *     resolution through the view path.
   *   - Otherwise we forward to `delegate.loadTable`. That re-fetches via `UCProxy` so
   *     DeltaCatalog (when present) can wrap the result and provide its Delta-specific
   *     reader; views never need that wrapping, so we skip Delta entirely for them. The
   *     extra `getTable` is intentional and avoids leaking Delta-specific behavior into the
   *     view path.
   *
   * The orthogonal `loadTable` / `loadView` overrides remain available: `loadTable` uses the
   * Delta-aware delegate path and rejects view-like rows; `loadView` delegates to
   * `UCProxy`, which inherits the `TableViewCatalog` default that derives from
   * `UCProxy.loadTableOrView` and rejects table rows.
   */
  override def loadTableOrView(ident: Identifier): Table = {
    val t = ucProxy.getUcTable(ident)
    if (UCSingleCatalog.isViewLikeTableType(t.getTableType)) {
      new MetadataTable(
        ucProxy.toViewInfo(t),
        // Spec recommends ident.toString() for the wrapper's `name()`; the resolver and
        // DESCRIBE TABLE EXTENDED render the multi-part v2 identifier consistently with
        // every other catalog this way.
        ident.toString)
    } else {
      delegate.loadTable(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      tableInfo: TableInfo): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    delegate.createTable(ident, tableInfo)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION)
    if (hasExternalClause && !hasLocationClause) {
      throw new ApiException("Cannot create EXTERNAL TABLE without location.")
    }

    if (UCSingleCatalog.isManagedDeltaTable(properties, ident)) {
      validateManagedDeltaCreateProperties(properties)
      val newProps = stageManagedDeltaTableAndGetProps(ident, properties)
      delegate.createTable(ident, columns, partitions, newProps)
    } else if (hasLocationClause) {
      val newProps = prepareExternalTableProperties(properties)
      delegate.createTable(ident, columns, partitions, newProps)
    } else {
      // TODO: for path-based tables, Spark should generate a location property using the qualified
      //       path string.
      delegate.createTable(ident, columns, partitions, properties)
    }
  }

  /** Prepares properties for managed table creation (staging table + credentials). */
  private def stageManagedDeltaTableAndGetProps(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String] = {
    // Get staging table location and table id from UC
    val createStagingTable = new CreateStagingTable()
      .catalogName(name())
      .schemaName(ident.namespace().head)
      .name(ident.name())
    val stagingTableInfo = tablesApi.createStagingTable(createStagingTable)
    val stagingLocation = stagingTableInfo.getStagingLocation
    val stagingTableId = stagingTableInfo.getId

    val newProps = new util.HashMap[String, String]
    newProps.putAll(properties)
    newProps.put(TableCatalog.PROP_LOCATION, stagingTableInfo.getStagingLocation)
    // Set the UC-assigned table ID so Delta can preserve table identity.
    newProps.put(UCTableProperties.UC_TABLE_ID_KEY, stagingTableInfo.getId)
    // `PROP_IS_MANAGED_LOCATION` is used to indicate that the table location is not
    // user-specified but system-generated, which is exactly the case here.
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")

    val temporaryCredentials = temporaryCredentialsApi.generateTemporaryTableCredentials(
      new GenerateTemporaryTableCredential().tableId(stagingTableId).operation(TableOperation.READ_WRITE))
    val credentialProps = UCCredentialHadoopConfs
      .builder(uri.toString, CatalogUtils.stringToURI(stagingLocation).getScheme)
      .tokenProvider(tokenProvider)
      .initialCredentials(temporaryCredentials)
      .enableCredentialRenewal(renewCredEnabled)
      .enableCredentialScopedFs(credScopedFsEnabled)
      .hadoopConf(UCSingleCatalog.sessionHadoopConf())
      .buildForTable(stagingTableId, TableOperation.READ_WRITE)
    UCSingleCatalog.setCredentialProps(newProps, credentialProps)
    newProps
  }

  /**
   * Checks that the user-supplied table properties are valid for creating a new UC-managed Delta
   * table.
   *
   * In this path, UC expects the caller to provide only user-controlled properties. It rejects
   * properties that UC itself assigns during staging, such as the UC table ID and the
   * managed-location marker. It also requires the catalog-managed Delta feature flag to be present
   * and set to the supported value, because that flag determines whether the new table is created
   * with the coordinated-commit behavior expected for UC-managed Delta tables.
   */
  private def validateManagedDeltaCreateProperties(properties: util.Map[String, String]): Unit = {
    rejectSystemManagedProperties(properties)
    if (!properties.containsKey(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW)) {
      throw new ApiException(
        s"Managed table creation requires table property " +
          s"'${UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW}'=" +
          s"'${UCTableProperties.DELTA_CATALOG_MANAGED_VALUE}'" +
          s" to be set.")
    }
    Option(properties.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW))
      .filter(_ != UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
      .foreach(v => throw new ApiException(
        s"Invalid property value '$v' for '${UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW}'."))
  }

  /**
   * Builds the property map for replacing an existing UC-managed Delta table.
   *
   * Instead of staging a brand-new managed table, this path starts from the current table metadata
   * returned by UC and prepares the properties needed to write back to that same managed table. It
   * preserves the catalog-managed marker, marks the location as system-managed, and vends fresh
   * READ_WRITE credentials for the existing table. It does not re-send the UC table ID as a
   * caller-provided property; Delta is expected to preserve the existing table identity from the
   * current snapshot during replace.
   */
  private def loadExistingManagedTablePropsForReplace(
      ident: Identifier,
      tableInfo: TableInfo,
      properties: util.Map[String, String],
      operation: String): util.Map[String, String] = {
    val fullTableName = UCSingleCatalog.fullTableNameForApi(name(), ident)

    // First, ensure the caller is not trying to override system-managed properties.
    rejectSystemManagedProperties(properties)
    Option(properties.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW))
      .filter(_ != UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
      .foreach(_ => throw new ApiException(
        s"Cannot override property '${UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW}'."))
    if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      throw new ApiException(
        s"$operation cannot specify property '${TableCatalog.PROP_LOCATION}' " +
          s"on an existing UC-managed Delta table.")
    }
    // Second, make sure UC says this is an existing catalog-managed Delta table and that the
    // metadata we need to reuse it for replace (storage location and table ID) is present.
    if (!isCatalogManagedDeltaTable(tableInfo)) {
      throw new UnsupportedOperationException(
        s"$operation is only supported for catalog-managed UC Delta tables")
    }
    val tableLocation = tableInfo.getStorageLocation
    val tableId = tableInfo.getTableId
    if (tableLocation == null || tableLocation.isEmpty) {
      throw new ApiException(
        s"Invalid table metadata for $fullTableName: storageLocation must be set")
    }
    if (tableId == null || tableId.isEmpty) {
      throw new ApiException(
        s"Invalid table metadata for $fullTableName: tableId must be set")
    }
    // Third, build the properties Delta needs in order to write back to the current managed table.
    val existingProvider = tableInfo.getDataSourceFormat.getValue.toLowerCase(Locale.ROOT)
    Option(properties.get(TableCatalog.PROP_PROVIDER))
      .filterNot(_.equalsIgnoreCase(existingProvider))
      .foreach(provider => throw new ApiException(
        s"$operation is only supported for Unity Catalog managed Delta tables and requires " +
          s"USING DELTA. Cannot change table format from " +
          s"${existingProvider.toUpperCase(Locale.ROOT)} to " +
          s"${provider.toUpperCase(Locale.ROOT)} for $fullTableName."))
    val newProps = new util.HashMap[String, String]
    newProps.putAll(properties)
    newProps.put(TableCatalog.PROP_PROVIDER, existingProvider)
    // Preserve the catalog-managed marker on the properties passed to Delta for replace.
    newProps.put(
      UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
      UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
    // Location intentionally omitted; Delta resolves it from the existing table snapshot.
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")

    // Finally, vend fresh READ_WRITE credentials for the existing table location.
    val temporaryCredentials = temporaryCredentialsApi.generateTemporaryTableCredentials(
      new GenerateTemporaryTableCredential()
        .tableId(tableId).operation(TableOperation.READ_WRITE))
    val tableUriScheme = new Path(tableLocation).toUri.getScheme
    val credentialProps = UCCredentialHadoopConfs.builder(uri.toString, tableUriScheme)
      .tokenProvider(tokenProvider)
      .initialCredentials(temporaryCredentials)
      .enableCredentialRenewal(renewCredEnabled)
      .enableCredentialScopedFs(credScopedFsEnabled)
      .hadoopConf(UCSingleCatalog.sessionHadoopConf())
      .buildForTable(tableId, TableOperation.READ_WRITE)
    UCSingleCatalog.setCredentialProps(newProps, credentialProps)
    newProps
  }

  private def rejectSystemManagedProperties(properties: util.Map[String, String]): Unit = {
    List(UCTableProperties.UC_TABLE_ID_KEY, TableCatalog.PROP_IS_MANAGED_LOCATION)
      .filter(properties.containsKey(_))
      .foreach(p => throw new ApiException(s"Cannot specify property '$p'."))
  }

  private def isCatalogManagedDeltaTable(tableInfo: TableInfo): Boolean = {
    val tableProperties = Option(tableInfo.getProperties)
    tableInfo.getTableType == TableType.MANAGED &&
    tableInfo.getDataSourceFormat == DataSourceFormat.DELTA &&
    tableProperties.exists(
      _.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW) ==
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
  }

  /** Prepares properties for external table creation (path credentials). */
  private def prepareExternalTableProperties(
      properties: util.Map[String, String]): util.Map[String, String] = {
    val location = properties.get(TableCatalog.PROP_LOCATION)
    assert(location != null)
    val cred = temporaryCredentialsApi.generateTemporaryPathCredentials(
      new GenerateTemporaryPathCredential().url(location).operation(PathOperation.PATH_CREATE_TABLE))
    val newProps = new util.HashMap[String, String]
    newProps.putAll(properties)

    val credentialProps = UCCredentialHadoopConfs
      .builder(uri.toString, CatalogUtils.stringToURI(location).getScheme)
      .tokenProvider(tokenProvider)
      .initialCredentials(cred)
      .enableCredentialRenewal(renewCredEnabled)
      .enableCredentialScopedFs(credScopedFsEnabled)
      .hadoopConf(UCSingleCatalog.sessionHadoopConf())
      .buildForPath(location, PathOperation.PATH_CREATE_TABLE)

    UCSingleCatalog.setCredentialProps(newProps, credentialProps)
    newProps
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    throw new AssertionError("deprecated `createTable` should not be called")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = delegate.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  override def listNamespaces(): Array[Array[String]] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    delegate.asInstanceOf[DelegatingCatalogExtension].loadNamespaceMetadata(namespace)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    delegate.asInstanceOf[DelegatingCatalogExtension].createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    delegate.asInstanceOf[DelegatingCatalogExtension].alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    delegate.asInstanceOf[DelegatingCatalogExtension].dropNamespace(namespace, cascade)
  }

  /** Only called for REPLACE TABLE and RTAS */
  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    val stagingCatalog = requireStagingCatalog("REPLACE TABLE")
    val existingTable = resolveExistingTableForReplace(ident, allowMissingTable = false)
    val newProps = loadExistingManagedTablePropsForReplace(
      ident,
      existingTable.get,
      properties,
      "REPLACE TABLE")
    UCSingleCatalog.requireProviderSpecified("REPLACE TABLE", newProps)
    stagingCatalog.stageReplace(ident, schema, partitions, newProps)
  }

  /** Only called for CREATE OR REPLACE TABLE ... [AS SELECT] */
  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    val stagingCatalog = requireStagingCatalog("CREATE OR REPLACE TABLE")
    val existingTable = resolveExistingTableForReplace(ident, allowMissingTable = true)
    val newProps = existingTable.map { tableInfo =>
      // Replacing existing table.
      loadExistingManagedTablePropsForReplace(
        ident,
        tableInfo,
        properties,
        "CREATE OR REPLACE TABLE")
    }.getOrElse {
      // Creating a new table.
      validateManagedDeltaCreateProperties(properties)
      stageManagedDeltaTableAndGetProps(ident, properties)
    }
    UCSingleCatalog.requireProviderSpecified("CREATE OR REPLACE TABLE", newProps)
    stagingCatalog.stageCreateOrReplace(ident, schema, partitions, newProps)
  }

  /**
   * Resolves the existing UC table metadata for REPLACE / CREATE OR REPLACE.
   */
  private def resolveExistingTableForReplace(
      ident: Identifier,
      allowMissingTable: Boolean): Option[TableInfo] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val fullTableName = UCSingleCatalog.fullTableNameForApi(name(), ident)
    try {
      Some(tablesApi.getTable(fullTableName,
        /* readStreamingTableAsManaged = */ false,
        /* readMaterializedViewAsManaged = */ false))
    } catch {
      case e: ApiException if e.getCode == 404 && allowMissingTable => None
      case e: ApiException if e.getCode == 404 => throw new NoSuchTableException(ident)
    }
  }

  private def requireStagingCatalog(operation: String): StagingTableCatalog = delegate match {
    case catalog: StagingTableCatalog => catalog
    case _ => throw new UnsupportedOperationException(s"$operation is not supported")
  }

  /** Only called for CTAS */
  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val stagingCatalog = requireStagingCatalog("CREATE TABLE AS SELECT (CTAS)")
    if (UCSingleCatalog.isManagedDeltaTable(properties, ident)) {
      val newProps = stageManagedDeltaTableAndGetProps(ident, properties)
      stagingCatalog.stageCreate(ident, schema, partitions, newProps)
    } else if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      val newProps = prepareExternalTableProperties(properties)
      stagingCatalog.stageCreate(ident, schema, partitions, newProps)
    } else {
      stagingCatalog.stageCreate(ident, schema, partitions, properties)
    }
  }
}

object UCSingleCatalog {
  val LOAD_DELTA_CATALOG = ThreadLocal.withInitial[Boolean](() => true)
  val DELTA_CATALOG_LOADED = ThreadLocal.withInitial[Boolean](() => false)

  /**
   * Returns the current session's Hadoop configuration.
   *
   * Passed to {@code UCCredentialHadoopConfs.Builder#hadoopConf} so
   * that the builder can look up any existing {@code fs.<scheme>.impl} values before
   * overriding them with the credential-scoped filesystem wrapper.
   */
  def sessionHadoopConf(): Configuration = {
    SparkSession.getActiveSession
      .map(_.sparkContext.hadoopConfiguration)
      .getOrElse(new Configuration())
  }

  def setCredentialProps(props: util.HashMap[String, String],
                         credentialProps: util.Map[String, String]): Unit = {
    props.putAll(credentialProps)
    // TODO: Delta requires the options to be set twice in the properties, with and without the
    //       `option.` prefix. We should revisit this in Delta.
    val prefix = TableCatalog.OPTION_PREFIX
    props.putAll(credentialProps.map {
      case (k, v) => (prefix + k, v)
    }.asJava)
  }

  def requireProviderSpecified(
      operation: String,
      properties: util.Map[String, String]): Unit = {
    Preconditions.checkArgument(
      properties.get(TableCatalog.PROP_PROVIDER) != null,
      "%s requires USING <format> (for example, USING DELTA)",
      operation)
  }

  /**
   * Determines whether a table should be created as a managed table.
   *
   * A table is considered managed if it has no EXTERNAL clause, no LOCATION clause,
   * and is not a path-based table (e.g., parquet.`/file/path`).
   *
   * @param properties the table properties from the CREATE TABLE command
   * @param ident the table identifier
   * @return true if the table should be managed, false otherwise
   */
  private def isManagedDeltaTable(properties: util.Map[String, String], ident: Identifier): Boolean = {
    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION)
    val isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute
    !hasExternalClause && !hasLocationClause && !isPathTable
  }

  def checkUnsupportedNestedNamespace(namespace: Array[String]): Unit = {
    if (namespace.length > 1) {
      throw new ApiException("Nested namespaces are not supported: " + namespace.mkString("."))
    }
  }

  /**
   * Constructs a fully qualified table name for Unity Catalog API calls.
   *
   * This method creates a three-part name in the format `catalog.schema.table` by combining
   * the catalog name with the schema name (from the identifier's namespace) and table name.
   * It is NOT backtick quoted like what is usually used in SQL statements even if the names have
   * special characters like hyphens.
   *
   * Example:
   * catalogName=catalog, ident=(schema, table): it returns "catalog.schema.table"
   * catalogName=cata-log, ident=(sche-ma, ta-ble): it returns "cata-log.sche-ma.ta-ble" (no quote)
   * catalogName=catalog, ident=((schema1, schema2), table): it throws
   *   ApiException(Nested namespace not supported)
   *
   * @param catalogName the name of the catalog
   * @param ident the table identifier containing the namespace (schema) and table name
   * @return a fully qualified table name in the format "catalog.schema.table"
   * @throws ApiException if the identifier contains nested namespaces (more than one level)
   */
  def fullTableNameForApi(catalogName: String, ident: Identifier): String = {
    checkUnsupportedNestedNamespace(ident.namespace())
    Seq(catalogName, ident.namespace()(0), ident.name()).mkString(".")
  }

  /**
   * Single source of truth for which UC `TableType` values the connector treats as
   * view-like, and how each maps onto Spark's `TableSummary` view-type strings:
   *
   *   - `Some(s)` means: appears in `listViews`, `loadTableOrView` builds a `ViewInfo`
   *     wrapped in `MetadataTable(... s)`, and `createView` accepts `s` as the requested
   *     `TableSummary` table-type string.
   *   - `None` means: appears in `listViews` (and is filtered out of `listTables`) so the
   *     UC server-managed lifecycle is not mis-classified as a table, but Spark has no
   *     `TableSummary` constant for it yet, so loading or creating fails with a clear
   *     "no Spark view type" error.
   *
   * Adding a future view kind = one new entry here. The three predicates below derive
   * from this map, so listing / dispatch / createView reject all stay in sync.
   */
  private val viewLikeUcTypes: Map[TableType, Option[String]] = Map(
    TableType.METRIC_VIEW -> Some(TableSummary.METRIC_VIEW_TABLE_TYPE),
    TableType.MATERIALIZED_VIEW -> None
  )

  /** Used by `listTables` / `listViews` / `loadTableOrView` / `dropTable` to dispatch by surface. */
  def isViewLikeTableType(tableType: TableType): Boolean = viewLikeUcTypes.contains(tableType)

  /**
   * Used by `toViewInfo` to label the wrapped `MetadataTable` with the right Spark
   * `TableSummary` table-type. Throws for view-like UC types we list but cannot yet load
   * (e.g. `MATERIALIZED_VIEW` until Spark adds a `TableSummary.MATERIALIZED_VIEW_TABLE_TYPE`).
   */
  def ucTableTypeToSparkViewType(tableType: TableType): String =
    viewLikeUcTypes.get(tableType).flatten.getOrElse(
      throw new UnsupportedOperationException(
        s"No Spark TableSummary view-type string for UC table type: $tableType"))

  /**
   * Reverse lookup: given a Spark `TableSummary` view-type string supplied by the caller
   * via `ViewInfo.properties().get(PROP_TABLE_TYPE)`, return the corresponding UC
   * `TableType` to stamp on `CreateTable.tableType`. Computed once from `viewLikeUcTypes`.
   */
  private lazy val sparkViewTypeToUcType: Map[String, TableType] =
    viewLikeUcTypes.collect { case (ucType, Some(sparkType)) => sparkType -> ucType }

  def sparkViewTypeToUcTableType(sparkType: String): Option[TableType] =
    sparkViewTypeToUcType.get(sparkType)

  /**
   * Used by view-side commands (`createView`, `dropView`, ...) to gate by whether the
   * connector supports the given view kind. Two overloads cover both call sites:
   *
   *   - `String` overload: caller has a Spark `TableSummary` view-type string (e.g. from
   *     `ViewInfo.properties().get(PROP_TABLE_TYPE)` on the create path).
   *   - `TableType` overload: caller has a UC `TableType` (e.g. from `getUcTable(...)` on
   *     the drop path).
   *
   * Both return `true` iff the kind is in `viewLikeUcTypes` AND has a Spark mapping
   * (i.e. `Some(_)` in the map). Listed-but-unmapped kinds (`None`) are filtered into
   * `listViews` for surface correctness but rejected from view CRUD commands.
   */
  def isViewCommandsSupportedTableType(sparkType: String): Boolean =
    sparkViewTypeToUcType.contains(sparkType)

  def isViewCommandsSupportedTableType(ucType: TableType): Boolean =
    viewLikeUcTypes.get(ucType).exists(_.isDefined)
}

// An internal proxy to talk to the UC client.
private class UCProxy(
    uri: URI,
    tokenProvider: TokenProvider,
    renewCredEnabled: Boolean,
    credScopedFsEnabled: Boolean,
    serverSidePlanningEnabled: Boolean,
    apiClient: ApiClient,
    tablesApi: TablesApi,
    temporaryCredentialsApi: TemporaryCredentialsApi)
  extends TableViewCatalog
  with SupportsNamespaces
  with Logging {
  private[this] var name: String = null
  private[this] var schemasApi: SchemasApi = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.name = name
    schemasApi = new SchemasApi(apiClient)
  }

  override def name(): String = {
    assert(this.name != null)
    this.name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    // Per the TableViewCatalog contract, listTables must return tables only -- view-like UC
    // table types (metric views, materialized views) belong on listViews.
    listUcTables(namespace)
      .filterNot(t => UCSingleCatalog.isViewLikeTableType(t.getTableType))
      .map(table => Identifier.of(namespace, table.getName))
      .toArray
  }

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    // Symmetric with listTables: list every UC view-like type so future view kinds (e.g.
    // materialized views) start showing up here without a touch on the listing code.
    listUcTables(namespace)
      .filter(t => UCSingleCatalog.isViewLikeTableType(t.getTableType))
      .map(table => Identifier.of(namespace, table.getName))
      .toArray
  }

  private def listUcTables(namespace: Array[String]): Seq[UCTableInfo] = {
    val catalogName = this.name
    val schemaName = namespace.head
    val tables = ArrayBuffer.empty[UCTableInfo]
    var pageToken: String = null
    do {
      val response = tablesApi.listTables(catalogName, schemaName, /* limit */ 0, pageToken)
      tables ++= response.getTables.asScala
      pageToken = response.getNextPageToken
    } while (pageToken != null && pageToken.nonEmpty)
    tables.toSeq
  }

  private[spark] def getUcTable(ident: Identifier): UCTableInfo = {
    try {
      tablesApi.getTable(
        UCSingleCatalog.fullTableNameForApi(this.name, ident),
        /* readStreamingTableAsManaged = */ true,
        /* readMaterializedViewAsManaged = */ true)
    } catch {
      case e: ApiException if e.getCode == 404 =>
        throw new NoSuchTableException(ident)
    }
  }

  // Single-RPC perf path for `TableViewCatalog`. Calls UC `getTable` once and dispatches by
  // `TableType`: a view-like row is wrapped in `MetadataTable(ViewInfo)`; everything else
  // goes through `loadV1Table` (which generates temp credentials).
  //
  // `loadView`, `loadTable`, `tableExists`, and `viewExists` are inherited from
  // `TableViewCatalog`'s defaults that derive from this method (see TableViewCatalog.java),
  // matching the upstream `InMemoryTableViewCatalog` pattern. There is no need to override
  // them -- they unwrap / discriminate the `MetadataTable + ViewInfo` shape automatically.
  override def loadTableOrView(ident: Identifier): Table = {
    val t = getUcTable(ident)
    if (UCSingleCatalog.isViewLikeTableType(t.getTableType)) {
      new MetadataTable(toViewInfo(t), ident.toString)
    } else {
      loadV1Table(ident, t)
    }
  }

  private def loadV1Table(
      ident: Identifier,
      t: UCTableInfo): Table = {
    val identifier = TableIdentifier(t.getName, Some(t.getSchemaName), Some(t.getCatalogName))
    val partitionCols = scala.collection.mutable.ArrayBuffer.empty[(String, Int)]
    val fields = t.getColumns.asScala.map { col =>
      Option(col.getPartitionIndex).foreach { index =>
        partitionCols += col.getName -> index
      }
      StructField(col.getName, DataType.fromDDL(col.getTypeText), col.getNullable)
        .withComment(col.getComment)
    }.toArray
    val locationUri = CatalogUtils.stringToURI(t.getStorageLocation)
    val tableId = t.getTableId
    var tableOp = TableOperation.READ_WRITE
    val temporaryCredentials = {
      try {
        temporaryCredentialsApi
          .generateTemporaryTableCredentials(
            // TODO: at this time, we don't know if the table will be read or written. For now we always
            //       request READ_WRITE credentials as the server doesn't distinguish between READ and
            //       READ_WRITE credentials as of today. When loading a table, Spark should tell if it's
            //       for read or write, we can request the proper credential after fixing Spark.
            new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp)
          )
      } catch {
        case e: ApiException =>
          logWarning(s"READ_WRITE credential generation failed for table $identifier: ${e.getMessage}")
          try {
            tableOp = TableOperation.READ
            temporaryCredentialsApi
              .generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp)
              )
          } catch {
            case e: ApiException =>
              logWarning(s"READ credential generation failed for table $identifier: ${e.getMessage}")
              if (serverSidePlanningEnabled) null else throw e
          }
      }
    }

    if (serverSidePlanningEnabled && temporaryCredentials == null) {
      enableServerSidePlanningConfig(identifier)
    }

    val extraSerdeProps = if (temporaryCredentials == null) {
      Map.empty[String, String].asJava
    } else {
      UCCredentialHadoopConfs.builder(uri.toString, locationUri.getScheme)
        .tokenProvider(tokenProvider)
        .initialCredentials(temporaryCredentials)
        .enableCredentialRenewal(renewCredEnabled)
        .enableCredentialScopedFs(credScopedFsEnabled)
        .hadoopConf(UCSingleCatalog.sessionHadoopConf())
        .buildForTable(tableId, tableOp)
    }

    val sparkTable = CatalogTable(
      identifier,
      tableType = if (t.getTableType == TableType.MANAGED) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      },
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(locationUri),
        properties = t.getProperties.asScala.toMap ++ extraSerdeProps
      ),
      schema = StructType(fields),
      provider = Some(t.getDataSourceFormat.getValue.toLowerCase()),
      createTime = t.getCreatedAt,
      tracksPartitionsInCatalog = false,
      partitionColumnNames = partitionCols.sortBy(_._2).map(_._1).toSeq
    )
    // Spark separates table lookup and data source resolution. To support Spark native data
    // sources, here we return the `V1Table` which only contains the table metadata. Spark will
    // resolve the data source and create scan node later.
    Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
      .getDeclaredConstructor(classOf[CatalogTable])
      .newInstance(sparkTable)
      .asInstanceOf[Table]
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val properties: util.Map[String, String] = info.properties()
    val sparkTableType = properties.get(TableCatalog.PROP_TABLE_TYPE)
    val ucTableType = UCSingleCatalog.sparkViewTypeToUcTableType(sparkTableType).getOrElse {
      throw new ApiException(
        s"Unity Catalog does not support creating $sparkTableType via ViewCatalog.createView")
    }

    val ct = new CreateTable()
      .name(ident.name())
      .schemaName(ident.namespace().head)
      .catalogName(this.name)
      .tableType(ucTableType)
      .viewDefinition(info.queryText())

    Option(properties.get(TableCatalog.PROP_COMMENT)).foreach(ct.setComment)
    Option(info.viewDependencies()).foreach { deps =>
      ct.setViewDependencies(UCProxy.toUcDependencyList(deps))
    }
    ct.setColumns(UCProxy.buildColumnInfos(info, convertDataTypeToTypeName).asJava)

    val propertiesToServer = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!UCTableProperties.V2_TABLE_PROPERTIES.contains(k)) {
        propertiesToServer.put(k, v)
      }
    }
    info.sqlConfigs().asScala.foreach { case (k, v) =>
      propertiesToServer.put(CatalogTable.VIEW_SQL_CONFIG_PREFIX + k, v)
    }
    ct.setProperties(propertiesToServer)

    try {
      tablesApi.createTable(ct)
    } catch {
      case e: ApiException if e.getCode == 409 =>
        throw new ViewAlreadyExistsException(ident)
    }
    loadView(ident)
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    throw new UnsupportedOperationException("Replacing a view is not supported yet")
  }

  override def dropView(ident: Identifier): Boolean = {
    val t = try {
      getUcTable(ident)
    } catch {
      case _: NoSuchTableException => return false
    }
    // Per the TableViewCatalog passive-filtering contract, dropView must return false (no
    // delete) when the entry exists but isn't a view kind we manage via view commands.
    // Same gate as createView -- one source of truth.
    if (!UCSingleCatalog.isViewCommandsSupportedTableType(t.getTableType)) {
      return false
    }
    tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident)) == 200
  }

  // UC OSS does not currently expose a rename endpoint for tables or views; mirror the
  // existing `renameTable` stub below until UC adds a rename API.
  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a view is not supported yet")
  }

  // Spark 4.2's `TableViewCatalog` made `createTable(ident, TableInfo)` the canonical entry
  // point; the legacy 4-arg method below carries all the create-table logic, which keeps the
  // diff vs. main scoped to "add the new overload + 409 -> TableAlreadyExistsException catch
  // required by TableViewCatalog's active-rejection contract".
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    createTable(ident, tableInfo.schema(), tableInfo.partitions(), tableInfo.properties())
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    UCSingleCatalog.requireProviderSpecified("CREATE TABLE", properties)

    val createTable = new CreateTable()
    createTable.setName(ident.name())
    createTable.setSchemaName(ident.namespace().head)
    createTable.setCatalogName(this.name)

    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val storageLocation = properties.get(TableCatalog.PROP_LOCATION)
    assert(storageLocation != null, "location should either be user specified or system generated.")
    val isManagedLocation = Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
      .exists(_.equalsIgnoreCase("true"))
    val format = properties.get("provider")
    if (isManagedLocation) {
      assert(!hasExternalClause, "location is only generated for managed tables.")
      if (!format.equalsIgnoreCase(DataSourceFormat.DELTA.name)) {
        throw new ApiException("Unity Catalog does not support non-Delta managed table.")
      }
      createTable.setTableType(TableType.MANAGED)
    } else {
      createTable.setTableType(TableType.EXTERNAL)
    }
    createTable.setStorageLocation(storageLocation)

    val partitionColNames: Seq[String] = partitions.flatMap { t =>
      t.name() match {
        case "identity" =>
          val fieldNames = t.references().flatMap(_.fieldNames())
          require(fieldNames.length == 1,
            s"Expected single-field partition reference but got: ${fieldNames.mkString(".")}")
          Some(fieldNames.head)
        case "cluster_by" =>
          None
        case other =>
          throw new ApiException(s"Unsupported partition transform: $other")
      }
    }.toSeq
    val columns: Seq[ColumnInfo] = schema.fields.toSeq.zipWithIndex.map { case (field, i) =>
      val column = new ColumnInfo()
      column.setName(field.name)
      if (field.getComment().isDefined) {
        column.setComment(field.getComment.get)
      }
      column.setNullable(field.nullable)
      column.setTypeText(field.dataType.catalogString)
      column.setTypeName(convertDataTypeToTypeName(field.dataType))
      column.setTypeJson(field.dataType.json)
      column.setPosition(i)
      val partitionIdx = partitionColNames.indexWhere(_.equalsIgnoreCase(field.name))
      if (partitionIdx >= 0) column.setPartitionIndex(partitionIdx)
      column
    }
    val comment = Option(properties.get(TableCatalog.PROP_COMMENT))
    comment.foreach(createTable.setComment(_))
    createTable.setColumns(columns)
    createTable.setDataSourceFormat(convertDatasourceFormat(format))
    // Do not send the V2 table properties as they are made part of the `createTable` already.
    val propertiesToServer =
      properties.view.filterKeys(!UCTableProperties.V2_TABLE_PROPERTIES.contains(_)).toMap
    createTable.setProperties(propertiesToServer)
    try {
      tablesApi.createTable(createTable)
    } catch {
      // TableViewCatalog's active-rejection contract: createTable must throw
      // TableAlreadyExistsException when a view (or another table, race) already sits at
      // `ident`. UC returns 409 in both cases.
      case e: ApiException if e.getCode == 409 =>
        throw new TableAlreadyExistsException(ident)
    }
    loadTable(ident)
  }

  private[spark] def toViewInfo(t: UCTableInfo): ViewInfo = {
    val columns = t.getColumns.asScala.map { col =>
      // typeJson holds the full StructField-shape JSON written by `buildColumnInfos`; parse
      // it back through `UCColumnJson` so dataType, nullable, comment, and metadata flow
      // through unchanged via Spark's already-public `DataType.fromJson` + `Column.create`.
      UCColumnJson.parseStructFieldJson(col.getTypeJson)
    }.toArray

    val props = new util.HashMap[String, String]()
    Option(t.getProperties).foreach(props.putAll)
    val sqlConfigs = extractSqlConfigs(props)

    val builder = new ViewInfo.Builder()
      .withColumns(columns)
      .withProperties(props)
      .withTableType(UCSingleCatalog.ucTableTypeToSparkViewType(t.getTableType))
      .withQueryText(t.getViewDefinition)
      .withCurrentCatalog(t.getCatalogName)
      .withCurrentNamespace(Array(t.getSchemaName))
      .withSqlConfigs(sqlConfigs)
      .withSchemaMode("UNSUPPORTED")
      .withQueryColumnNames(columns.map(_.name()))
    Option(t.getComment).foreach(builder.withComment)
    Option(t.getViewDependencies).foreach { ucDeps =>
      builder.withViewDependencies(UCProxy.fromUcDependencyList(ucDeps))
    }
    builder.build()
  }

  private def extractSqlConfigs(properties: util.Map[String, String]): util.Map[String, String] = {
    val configs = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (k.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX)) {
        configs.put(k.substring(CatalogTable.VIEW_SQL_CONFIG_PREFIX.length), v)
      }
    }
    configs
  }

  private def convertDatasourceFormat(format: String): DataSourceFormat = {
    format.toUpperCase match {
      case "PARQUET" => DataSourceFormat.PARQUET
      case "CSV" => DataSourceFormat.CSV
      case "DELTA" => DataSourceFormat.DELTA
      case "JSON" => DataSourceFormat.JSON
      case "ORC" => DataSourceFormat.ORC
      case "TEXT" => DataSourceFormat.TEXT
      case "AVRO" => DataSourceFormat.AVRO
      case _ => throw new ApiException("DataSourceFormat not supported: " + format)
    }
  }

  private def convertDataTypeToTypeName(dataType: DataType): ColumnTypeName = {
    dataType match {
      case _: BooleanType => ColumnTypeName.BOOLEAN
      case _: ByteType => ColumnTypeName.BYTE
      case _: ShortType => ColumnTypeName.SHORT
      case _: IntegerType => ColumnTypeName.INT
      case _: LongType => ColumnTypeName.LONG
      case _: FloatType => ColumnTypeName.FLOAT
      case _: DoubleType => ColumnTypeName.DOUBLE
      case _: DateType => ColumnTypeName.DATE
      case _: TimestampType => ColumnTypeName.TIMESTAMP
      case _: TimestampNTZType => ColumnTypeName.TIMESTAMP_NTZ
      case _: CharType => ColumnTypeName.CHAR
      case _: StringType | _: VarcharType => ColumnTypeName.STRING
      case _: BinaryType => ColumnTypeName.BINARY
      case _: DecimalType => ColumnTypeName.DECIMAL
      case _: DayTimeIntervalType | _: YearMonthIntervalType =>
        ColumnTypeName.INTERVAL
      case _: ArrayType => ColumnTypeName.ARRAY
      case _: StructType => ColumnTypeName.STRUCT
      case _: MapType => ColumnTypeName.MAP
      case _: NullType => ColumnTypeName.NULL
      case _: UserDefinedType[_] => ColumnTypeName.USER_DEFINED_TYPE
      case _: VariantType => ColumnTypeName.VARIANT
      case _ => ColumnTypeName.UNKNOWN_DEFAULT_OPEN_API
    }
  }

  /**
   * Enables server-side planning by setting the appropriate Spark config.
   * Called when credential vending fails but SSP is enabled, allowing Delta
   * to use server-side planning for data access instead of credentials from UC.
   */
  private def enableServerSidePlanningConfig(identifier: TableIdentifier): Unit = {
    SparkSession.getActiveSession match {
      case Some(spark) =>
        spark.conf.set("spark.databricks.delta.catalog.enableServerSidePlanning", "true")
        logInfo(
          s"Server-side planning enabled for table $identifier. " +
          s"Set spark.databricks.delta.catalog.enableServerSidePlanning=true. " +
          s"Proceeding with empty credentials. Delta will use server-side planning for data access."
        )
      case None =>
        logWarning(
          s"Server-side planning enabled for table $identifier but no active SparkSession found. " +
          s"Cannot set Spark config. Table access may fail."
        )
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    // Spark's `DROP TABLE` always lands in `TableCatalog.dropTable`, regardless of what kind
    // of object actually sits at `ident`. `TableViewCatalog` puts tables and views in a
    // shared identifier namespace, and the spec requires `dropTable` to passive-filter
    // view-like rows -- a `DROP TABLE <view>` must look like "nothing here" and return false
    // rather than delete the view. (`dropView` is symmetric: returns false for a table.)
    // We resolve the kind via UC `getTable` first so a view-like row never reaches
    // `tablesApi.deleteTable`.
    val t = try {
      getUcTable(ident)
    } catch {
      case _: NoSuchTableException => return false
    }
    if (UCSingleCatalog.isViewLikeTableType(t.getTableType)) {
      return false
    }
    val ret = tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident))
    if (ret == 200) true else false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  override def listNamespaces(): Array[Array[String]] = {
    val schemas = ArrayBuffer.empty[Array[String]]
    var pageToken: String = null
    do {
      val response = schemasApi.listSchemas(name, /* limit */ 0, pageToken)
      schemas ++= response.getSchemas.asScala.map(schema => Array(schema.getName))
      pageToken = response.getNextPageToken
    } while (pageToken != null && pageToken.nonEmpty)
    schemas.toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    throw new UnsupportedOperationException("Multi-layer namespace is not supported in Unity Catalog")
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    val schema = try {
      schemasApi.getSchema(name + "." + namespace(0))
    } catch {
      case e: ApiException if e.getCode == 404 =>
        throw new NoSuchNamespaceException(namespace)
    }
    // flatten the schema properties to a map, with the key prefixed by "properties:"
    val metadata = schema.getProperties.asScala.map {
      case (k, v) =>  SchemaInfo.JSON_PROPERTY_PROPERTIES + ":" + k -> v
    }
    metadata(SchemaInfo.JSON_PROPERTY_NAME) = schema.getName
    metadata(SchemaInfo.JSON_PROPERTY_CATALOG_NAME) = schema.getCatalogName
    metadata(SchemaInfo.JSON_PROPERTY_COMMENT) = schema.getComment
    metadata(SchemaInfo.JSON_PROPERTY_FULL_NAME) = schema.getFullName
    metadata(SchemaInfo.JSON_PROPERTY_CREATED_AT) = if (schema.getCreatedAt != null) {schema.getCreatedAt.toString} else {"null"}
    metadata(SchemaInfo.JSON_PROPERTY_UPDATED_AT) = if (schema.getUpdatedAt != null) {schema.getUpdatedAt.toString} else {"null"}
    metadata(SchemaInfo.JSON_PROPERTY_SCHEMA_ID) = schema.getSchemaId
    metadata.asJava
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    val createSchema = new CreateSchema()
    createSchema.setCatalogName(this.name)
    createSchema.setName(namespace.head)
    createSchema.setProperties(metadata)
    schemasApi.createSchema(createSchema)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("Renaming a namespace is not supported yet")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    schemasApi.deleteSchema(name + "." + namespace.head, cascade)
    true
  }
}

private object UCProxy {

  /**
   * Converts Spark's typed [[DependencyList]] into the wire-format UC [[UCDependencyList]].
   * Only `TableDependency` is currently translated; UC OSS does not persist function
   * dependencies for now.
   *
   * Spark's `TableDependency.nameParts` is the structural multi-part identifier (preserves
   * arity for multi-level-namespace catalogs); UC's wire format is the legacy
   * `tableFullName: String` -- a dot-joined flattening of the parts. The conversion is
   * lossy for identifiers containing literal `.` (the wire side has no quoting convention),
   * but UC's server-side dependency tracking has the same limitation today, so no fidelity
   * is lost vs. the pre-PR producer that emitted dot-joined strings directly.
   */
  def toUcDependencyList(
      sparkDeps: DependencyList)
      : UCDependencyList = {
    val ucDeps = new java.util.ArrayList[UCDependency]()
    sparkDeps.dependencies().foreach {
      case td: TableDependency =>
        ucDeps.add(new UCDependency()
          .table(new UCTableDependency()
            .tableFullName(td.nameParts().mkString("."))))
      case _ =>
      // UC OSS does not currently persist function dependencies; drop.
    }
    new UCDependencyList().dependencies(ucDeps)
  }

  /**
   * Converts the wire-format UC [[UCDependencyList]] back into Spark's typed
   * [[DependencyList]]. Splits UC's dot-joined `tableFullName` back into the structural
   * `nameParts` accepted by the new varargs `Dependency.table(String...)` factory; same
   * dot-flattening caveat as [[toUcDependencyList]].
   */
  def fromUcDependencyList(
      ucDeps: UCDependencyList)
      : DependencyList = {
    val list = Option(ucDeps.getDependencies)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
    val sparkDeps: Array[Dependency] = list.flatMap { d =>
      Option(d.getTable).map { td =>
        // `split("\\.", -1)` keeps trailing empty parts -- defensive against malformed
        // input from the wire (UC's server should never produce trailing dots).
        Dependency.table(td.getTableFullName.split("\\.", -1): _*)
          .asInstanceOf[Dependency]
      }
    }.toArray
    DependencyList.of(sparkDeps: _*)
  }

  /**
   * Builds the UC column-info list for `createView` from a Spark `TableInfo` (or its `ViewInfo`
   * subtype). Preserves nullability, comment, partition position, and the full Spark
   * `StructField` JSON in `typeJson` so column-level analyzer metadata (e.g. `metric_view.type`,
   * `metric_view.expr`) survives the round-trip through Unity Catalog. Matches the Databricks
   * runtime wire format (`TypeConversionUtils.toProto` -> `typeJson = Some(field.toJson)`).
   *
   * Iterates the typed `tableInfo.columns()` directly rather than going through
   * `tableInfo.schema()`, so we read each column's metadata via the public
   * `Column.metadataInJSON()` accessor. The `typeJson` is built by [[UCColumnJson]] with
   * json4s + Spark's already-public `DataType.json` -- no `StructField.json` /
   * `Column.fromStructField` Spark API addition needed.
   */
  def buildColumnInfos(
      tableInfo: TableInfo,
      convertDataTypeToTypeName: DataType => ColumnTypeName): Seq[ColumnInfo] = {
    val partitionColNames = extractPartitionColumnNames(tableInfo.partitions())
    tableInfo.columns().toSeq.zipWithIndex.map { case (col, i) =>
      val column = new ColumnInfo()
      column.setName(col.name)
      column.setNullable(col.nullable)
      column.setTypeText(col.dataType.catalogString)
      column.setTypeName(convertDataTypeToTypeName(col.dataType))
      column.setTypeJson(UCColumnJson.buildStructFieldJson(col))
      column.setPosition(i)
      Option(col.comment).foreach(column.setComment(_))
      val partitionIdx = partitionColNames.indexWhere(_.equalsIgnoreCase(col.name))
      if (partitionIdx >= 0) column.setPartitionIndex(partitionIdx)
      column
    }
  }

  private def extractPartitionColumnNames(partitions: Array[Transform]): Seq[String] = {
    if (partitions == null) return Seq.empty
    partitions.flatMap { t =>
      t.name() match {
        case "identity" =>
          val fieldNames = t.references().flatMap(_.fieldNames())
          require(fieldNames.length == 1,
            s"Expected single-field partition reference but got: ${fieldNames.mkString(".")}")
          Some(fieldNames.head)
        case "cluster_by" =>
          None
        case other =>
          throw new ApiException(s"Unsupported partition transform: $other")
      }
    }.toSeq
  }
}

/**
 * Helpers that round-trip a Spark V2 [[Column]] through the canonical "Spark `StructField`
 * shape" JSON that Unity Catalog stores in `ColumnInfo.type_json`. Wire-format compatible
 * with what Databricks Runtime emits via `StructField.toJson`.
 *
 * Implemented with json4s + Spark's already-public V2 surface only:
 *
 *   - [[Column.name]] / [[Column.dataType]] / [[Column.nullable]] / [[Column.comment]] /
 *     [[Column.metadataInJSON]] -- public accessors on the V2 catalog `Column` interface.
 *   - [[org.apache.spark.sql.types.DataType.json]] / [[DataType.fromJson]] -- public on
 *     Spark `DataType`.
 *   - [[Column.create]] (5-arg overload) -- public static factory on the V2 `Column`.
 *
 * No `private[sql]` helpers, no Spark API additions, no `StructField.json` /
 * `Column.fromStructField` dependency. The shape is just `{name, type, nullable, metadata}`.
 *
 * Comment handling mirrors Spark's StructField wire convention: comments live inside
 * `metadata` under the `"comment"` key on the wire, but on the in-memory `Column` they are
 * exposed via the dedicated [[Column.comment]] accessor. The helpers merge / strip the
 * `"comment"` key at the JSON boundary so callers never see it duplicated.
 */
private[spark] object UCColumnJson {

  /** Spark V2 -> wire: build the `StructField`-shape JSON for one [[Column]]. */
  def buildStructFieldJson(col: Column): String = {
    val typeNode: JValue = JsonMethods.parse(col.dataType.json)
    // metadataInJSON may be null for fields without analyzer-attached metadata.
    val baseMetadata: JObject = Option(col.metadataInJSON) match {
      case Some(s) => JsonMethods.parse(s).asInstanceOf[JObject]
      case None    => JObject(Nil)
    }
    val metadataNode: JObject = Option(col.comment) match {
      case Some(c) => baseMetadata ~ ("comment" -> c)
      case None    => baseMetadata
    }
    val fieldNode: JValue =
      ("name"     -> col.name) ~
        ("type"     -> typeNode) ~
        ("nullable" -> col.nullable) ~
        ("metadata" -> metadataNode)
    JsonMethods.compact(JsonMethods.render(fieldNode))
  }

  /**
   * Wire -> Spark V2: parse a `StructField`-shape JSON string back into a [[Column]].
   *
   * The `"comment"` key (if present in the wire metadata) is lifted out and passed to
   * [[Column.create]] as the dedicated `comment` arg, NOT left in `metadataInJSON` --
   * matches what Spark's own `CatalogV2Util.structFieldToV2Column` does on the v1 path.
   */
  def parseStructFieldJson(jsonStr: String): Column = {
    val parsed = JsonMethods.parse(jsonStr)
    val name = (parsed \ "name") match {
      case JString(s) => s
      case other      => throw new IllegalArgumentException(
        s"Expected string `name` in StructField JSON, got: $other")
    }
    val typeNode = parsed \ "type"
    val dataType = DataType.fromJson(JsonMethods.compact(JsonMethods.render(typeNode)))
    val nullable = (parsed \ "nullable") match {
      case JBool(b) => b
      case other    => throw new IllegalArgumentException(
        s"Expected boolean `nullable` in StructField JSON, got: $other")
    }
    val metadataObj = (parsed \ "metadata") match {
      case obj: JObject => obj
      case _            => JObject(Nil)
    }
    val (commentFields, otherFields) = metadataObj.obj.partition(_._1 == "comment")
    val comment = commentFields.headOption.map(_._2) match {
      case Some(JString(s)) => s
      case _                => null
    }
    val metadataInJSON = otherFields match {
      case Nil => null
      case xs  => JsonMethods.compact(JsonMethods.render(JObject(xs)))
    }
    Column.create(name, dataType, nullable, comment, metadataInJSON)
  }
}
