package io.unitycatalog.spark

import java.net.URI
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import io.unitycatalog.client.api.{SchemasApi, TablesApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{
  TableInfo => UCTableInfo,
  _
}
import io.unitycatalog.client.retry.JitterDelayRetryPolicy
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.hadoop.UCCredentialHadoopConfs
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.{PathOperation, TableOperation}
import io.unitycatalog.spark.auth.AuthConfigUtils
import io.unitycatalog.spark.compat.SparkCatalogCompatibility
import io.unitycatalog.spark.utils.OptionsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException, ViewAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.sparkproject.guava.base.Preconditions

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog
  extends StagingTableCatalog
  with SupportsNamespaces
  with UCSingleCatalogViewSupport
  with Logging {

  private[this] var uri: URI = null
  private[this] var tokenProvider: TokenProvider = null
  private[this] var renewCredEnabled: Boolean = false
  private[this] var credScopedFsEnabled: Boolean = true
  private[this] var apiClient: ApiClient = null;
  private[this] var tablesApi: TablesApi = null

  // TODO: move this Delta loading logic into a separate base class DeltaSupport

  // When true, callers that target managed Delta tables skip the legacy UC staging step here and
  // let the Delta catalog stage the table via the UC Delta API instead.
  private[this] var deltaRestApiEnabled: Boolean = false

  // Set to true after the bundled Delta library successfully loads in `initialize`. Used
  // (alongside `deltaRestApiEnabled` and a Delta version check) to decide whether managed
  // Delta creates can take the new path.
  private[this] var deltaCatalogLoaded: Boolean = false

  // The connector keeps two references to the catalog underneath:
  //
  //   - `delegate` is the *outermost* `TableCatalog` the connector exposes for table
  //     operations. When DeltaCatalog is on the classpath it wraps `ucProxy` via
  //     `DelegatingCatalogExtension`, so Delta can intercept managed / external Delta-table
  //     create / load paths and add Delta-specific behavior. When DeltaCatalog is not on the
  //     classpath, `delegate` is set to `ucProxy` itself so non-Delta table paths still work.
  //
  //   - `ucProxy` is the underlying UC REST client wrapped as a Spark `RelationCatalog`. It
  //     is always the real `UCProxy` instance so we can route view operations directly. Delta
  //     has no role in view handling, so view-side methods (`createView` / `loadView` /
  //     `dropView` / `listViews`) bypass `delegate` and call `ucProxy` directly, avoiding the
  //     need to unwrap the `DelegatingCatalogExtension` chain.
  // Protected so the per-Spark-version `UCSingleCatalogViewSupport` trait can read them.
  @volatile protected[spark] var delegate: TableCatalog = null
  @volatile protected[spark] var ucProxy: UCProxy = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val urlStr = options.get(OptionsUtil.URI)
    Preconditions.checkArgument(urlStr != null,
      "uri must be specified for Unity Catalog '%s'", name)
    uri = new URI(urlStr)
    tokenProvider = TokenProvider.create(AuthConfigUtils.buildAuthConfigs(options));
    renewCredEnabled = options.getBoolean(
      OptionsUtil.RENEW_CREDENTIAL_ENABLED, OptionsUtil.DEFAULT_RENEW_CREDENTIAL_ENABLED)
    credScopedFsEnabled = options.getBoolean(
      OptionsUtil.CRED_SCOPED_FS_ENABLED, OptionsUtil.DEFAULT_CRED_SCOPED_FS_ENABLED)
    val serverSidePlanningEnabled = options.getBoolean(
      OptionsUtil.SERVER_SIDE_PLANNING_ENABLED, OptionsUtil.DEFAULT_SERVER_SIDE_PLANNING_ENABLED)
    deltaRestApiEnabled = options.getBoolean(
      OptionsUtil.DELTA_API_ENABLED, OptionsUtil.DEFAULT_DELTA_API_ENABLED)

    apiClient = ApiClientFactory.createApiClient(
      JitterDelayRetryPolicy.builder().build(),uri, tokenProvider)
    tablesApi = new TablesApi(apiClient)
    ucProxy = new UCProxy(uri, tokenProvider, renewCredEnabled, credScopedFsEnabled,
      serverSidePlanningEnabled, apiClient, tablesApi)
    ucProxy.initialize(name, options)
    if (UCSingleCatalog.LOAD_DELTA_CATALOG.get()) {
      try {
        delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
        delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(ucProxy)
        delegate.initialize(name, options)
        UCSingleCatalog.DELTA_CATALOG_LOADED.set(true)
        deltaCatalogLoaded = true
      } catch {
        case e: ClassNotFoundException =>
          logWarning("DeltaCatalog is not available in the classpath", e)
          delegate = ucProxy
      }
    } else {
      delegate = ucProxy
    }
  }

  /** See [[DeltaVersionUtils.isDeltaRestApiReady]] for the predicate. */
  private def shouldUseDeltaAPI: Boolean =
    DeltaVersionUtils.isDeltaRestApiReady(deltaCatalogLoaded, deltaRestApiEnabled)

  /**
   * REPLACE / CREATE OR REPLACE delegation gate.
   *
   *   - Delegate when: UC Delta API is wired in AND request specifies Delta provider.
   *   - Existing-table validation (MANAGED, Delta, catalog-managed) is enforced by the
   *     Delta-side `buildReplaceProps` after loading; we do not pre-check it here.
   *   - Non-Delta REPLACE stays on the legacy path; its rejection error is unchanged.
   */
  private def shouldDelegateReplaceToDeltaApi(properties: util.Map[String, String]): Boolean = {
    shouldUseDeltaAPI && UCSingleCatalog.hasDeltaProvider(properties)
  }

  /**
   * Returns the properties UC should pass to the Delta catalog delegate for a managed Delta
   * CREATE / CTAS. Runs {@link #validateAndDefaultManagedDeltaCreateProperties} on the input
   * first. When the UC Delta API path is ready, we skip UC's legacy `createStagingTable` and
   * pass the validated properties through untouched -- the Delta catalog stages and finalizes
   * via the UC Delta API. Otherwise we fall back to the legacy staging path so older Delta
   * builds keep working.
   */
  private def managedDeltaCreatePropsForDelegate(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String] = {
    val validated = validateAndDefaultManagedDeltaCreateProperties(properties)
    if (shouldUseDeltaAPI) validated
    else stageManagedDeltaTableAndGetProps(ident, validated)
  }

  override def name(): String = delegate.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = delegate.listTables(namespace)

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident)

  override def loadTable(ident: Identifier, version:  String): Table = delegate.loadTable(ident, version)

  override def loadTable(ident: Identifier, timestamp:  Long): Table = delegate.loadTable(ident, timestamp)

  override def tableExists(ident: Identifier): Boolean = {
    delegate.tableExists(ident)
  }

  override def capabilities(): util.Set[TableCatalogCapability] = delegate.capabilities()

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

    if (UCSingleCatalog.isManagedTable(properties, ident)) {
      if (UCSingleCatalog.hasDeltaProvider(properties)) {
        // Managed Delta table
        val newProps = managedDeltaCreatePropsForDelegate(ident, properties)
        delegate.createTable(ident, columns, partitions, newProps)
      } else {
        // Managed (no LOCATION, no EXTERNAL) but not Delta: UC doesn't support non-Delta managed
        // tables. Surface the same friendly error the legacy staging path used to produce.
        throw new ApiException("Unity Catalog does not support non-Delta managed table.")
      }
    } else if (hasLocationClause) {
      val newProps = prepareExternalTableProperties(properties)
      delegate.createTable(ident, columns, partitions, newProps)
    } else {
      // Path-based identifiers (e.g. `delta`.`/tmp/foo`) fall through to the delegate.
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

    val credentialProps = UCCredentialHadoopConfs
      .builder(uri.toString, CatalogUtils.stringToURI(stagingLocation).getScheme)
      .addAppVersions(ApiClientFactory.appEngineVersions())
      .tokenProvider(tokenProvider)
      .apiClient(apiClient)
      .enableCredentialRenewal(renewCredEnabled)
      .enableCredentialScopedFs(credScopedFsEnabled)
      .hadoopConf(UCSingleCatalog.sessionHadoopConf())
      .buildForTable(stagingTableId, TableOperation.READ_WRITE)
    UCSingleCatalog.setCredentialProps(newProps, credentialProps)
    newProps
  }

  /**
   * Validates and normalizes the user-supplied table properties for creating a new UC-managed
   * Delta table, returning the properties to pass downstream.
   *
   * In this path, UC expects the caller to provide only user-controlled properties. It rejects
   * properties that UC itself assigns during staging, such as the UC table ID and the
   * managed-location marker. The catalog-managed Delta feature flag is automatically turned on.
   */
  private def validateAndDefaultManagedDeltaCreateProperties(
      properties: util.Map[String, String]): util.Map[String, String] = {
    rejectSystemManagedProperties(properties)
    Option(properties.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY)) match {
      case None =>
        val augmented = new util.HashMap[String, String](properties)
        augmented.put(
          UCTableProperties.DELTA_CATALOG_MANAGED_KEY,
          UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
        augmented
      case Some(v) if v == UCTableProperties.DELTA_CATALOG_MANAGED_VALUE =>
        properties
      case Some(v) =>
        throw new ApiException(
          s"Invalid property value '$v' for '${UCTableProperties.DELTA_CATALOG_MANAGED_KEY}'. " +
            s"Must be '${UCTableProperties.DELTA_CATALOG_MANAGED_VALUE}'.")
    }
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
      tableInfo: UCTableInfo,
      properties: util.Map[String, String],
      operation: String): util.Map[String, String] = {
    val fullTableName = UCSingleCatalog.fullTableNameForApi(name(), ident)

    // First, ensure the caller is not trying to override system-managed properties. Called
    // explicitly here (and again inside validateAndDefault below) so a system-managed override on
    // a REPLACE against a non-managed-Delta table still surfaces the precise error rather than
    // tripping the more generic "only supported for catalog-managed UC Delta tables" gate.
    rejectSystemManagedProperties(properties)
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
    // Deferred to here so the existing-table gate above can short-circuit for non-managed-Delta
    // REPLACE without paying the validate-default cost (and without auto-adding a Delta-only
    // property to a non-Delta property map).
    val validated = validateAndDefaultManagedDeltaCreateProperties(properties)
    val newProps = new util.HashMap[String, String]
    newProps.putAll(validated)
    newProps.put(TableCatalog.PROP_PROVIDER, existingProvider)
    // Location intentionally omitted; Delta resolves it from the existing table snapshot.
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")

    // Finally, vend fresh READ_WRITE credentials for the existing table location.
    val tableUriScheme = new Path(tableLocation).toUri.getScheme
    val credentialProps = UCCredentialHadoopConfs
      .builder(uri.toString, tableUriScheme)
      .addAppVersions(ApiClientFactory.appEngineVersions())
      .tokenProvider(tokenProvider)
      .apiClient(apiClient)
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

  private def isCatalogManagedDeltaTable(tableInfo: UCTableInfo): Boolean = {
    val tableProperties = Option(tableInfo.getProperties)
    tableInfo.getTableType == TableType.MANAGED &&
    tableInfo.getDataSourceFormat == DataSourceFormat.DELTA &&
    tableProperties.exists(
      _.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY) ==
        UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)
  }

  /** Prepares properties for external table creation (path credentials). */
  private def prepareExternalTableProperties(
      properties: util.Map[String, String]): util.Map[String, String] = {
    val location = properties.get(TableCatalog.PROP_LOCATION)
    assert(location != null)
    val newProps = new util.HashMap[String, String]
    newProps.putAll(properties)

    val credentialProps = UCCredentialHadoopConfs
      .builder(uri.toString, CatalogUtils.stringToURI(location).getScheme)
      .addAppVersions(ApiClientFactory.appEngineVersions())
      .tokenProvider(tokenProvider)
      .apiClient(apiClient)
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
    // Spark represents ALTER TABLE ... RENAME COLUMN as a structured TableChange here.
    if (changes.exists(_.isInstanceOf[TableChange.RenameColumn])) {
      throw new UnsupportedOperationException(
        "ALTER TABLE RENAME COLUMN is not supported for Unity Catalog tables yet")
    }
    delegate.alterTable(ident, changes: _*)
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
    if (shouldDelegateReplaceToDeltaApi(properties)) {
      // Defer load + augment to the Delta-side `buildReplaceProps`. UCSingleCatalog no longer
      // loads the existing table and prepare the properties for replace as this will be done by
      // Delta instead.
      return stagingCatalog.stageReplace(ident, schema, partitions, properties)
    }
    // The following becomes dead code when Delta API is turned on
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
    if (shouldDelegateReplaceToDeltaApi(properties)) {
      // Defer existence-probe + branch (create-staging vs replace) to the Delta catalog.
      // UCSingleCatalog no longer loads the existing table and prepare the properties for replace,
      // or create a staging table if table doesn't exist yet, as this will be done by Delta catalog
      // instead.
      return stagingCatalog.stageCreateOrReplace(ident, schema, partitions, properties)
    }
    // The following becomes dead code when Delta API is turned on
    val existingTable = resolveExistingTableForReplace(ident, allowMissingTable = true)
    val newProps = existingTable.map { tableInfo =>
      // Replacing existing table.
      loadExistingManagedTablePropsForReplace(
        ident,
        tableInfo,
        properties,
        "CREATE OR REPLACE TABLE")
    }.getOrElse {
      // Creating a new table -- route through the same gate as `createTable` so a fresh
      // CREATE OR REPLACE picks up the UC Delta API path when it's available.
      managedDeltaCreatePropsForDelegate(ident, properties)
    }
    UCSingleCatalog.requireProviderSpecified("CREATE OR REPLACE TABLE", newProps)
    stagingCatalog.stageCreateOrReplace(ident, schema, partitions, newProps)
  }

  /**
   * Resolves the existing UC table metadata for REPLACE / CREATE OR REPLACE.
   */
  private def resolveExistingTableForReplace(
      ident: Identifier,
      allowMissingTable: Boolean): Option[UCTableInfo] = {
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
    if (UCSingleCatalog.isManagedTable(properties, ident)) {
      if (UCSingleCatalog.hasDeltaProvider(properties)) {
        // Managed Delta table
        val newProps = managedDeltaCreatePropsForDelegate(ident, properties)
        stagingCatalog.stageCreate(ident, schema, partitions, newProps)
      } else {
        // Managed (no LOCATION, no EXTERNAL) but not Delta: UC doesn't support non-Delta managed
        // tables. Surface the same friendly error the legacy staging path used to produce.
        throw new ApiException("Unity Catalog does not support non-Delta managed table.")
      }
    } else if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      val newProps = prepareExternalTableProperties(properties)
      stagingCatalog.stageCreate(ident, schema, partitions, newProps)
    } else {
      stagingCatalog.stageCreate(ident, schema, partitions, properties)
    }
  }
}

object UCSingleCatalog {
  // DeltaCatalog loading status for testing purpose.
  val LOAD_DELTA_CATALOG = ThreadLocal.withInitial[Boolean](() => true)
  val DELTA_CATALOG_LOADED = ThreadLocal.withInitial[Boolean](() => false)

  /**
   * Returns the current session's Hadoop configuration, blended with any session-scoped
   * SQLConf overrides (this is what Spark SQL itself uses; see SPARK-23514).
   *
   * Passed to {@code UCCredentialHadoopConfs.Builder#hadoopConf} so that the builder can look up
   * any existing {@code fs.<scheme>.impl} values before overriding them with the credential-scoped
   * filesystem wrapper. Using {@code sessionState.newHadoopConf()} (instead of
   * {@code sparkContext.hadoopConfiguration}) ensures the lookup also sees Hadoop properties set
   * directly via {@code SparkSession.Builder.config("fs.<scheme>.impl", ...)} or {@code SET}
   * commands, not only those prefixed with {@code spark.hadoop.}.
   */
  def sessionHadoopConf(): Configuration = {
    SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
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
  private def isManagedTable(properties: util.Map[String, String], ident: Identifier): Boolean = {
    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION)
    val isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute
    !hasExternalClause && !hasLocationClause && !isPathTable
  }

  /** Returns true when `USING <format>` is `delta`. */
  def hasDeltaProvider(properties: util.Map[String, String]): Boolean =
    Option(properties.get(TableCatalog.PROP_PROVIDER))
      .exists(_.equalsIgnoreCase("delta"))

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

}

/** Internal signal that preserves a view row rejected by the table-only catalog surface. */
private[spark] final class ViewFoundDuringTableLoadException(
    ident: Identifier,
    val tableInfo: UCTableInfo)
  extends NoSuchTableException(ident)

// An internal proxy to talk to the UC client.
private[spark] class UCProxy(
    uri: URI,
    tokenProvider: TokenProvider,
    renewCredEnabled: Boolean,
    credScopedFsEnabled: Boolean,
    serverSidePlanningEnabled: Boolean,
    apiClient: ApiClient,
    protected[spark] val tablesApi: TablesApi)
  extends TableCatalog
  with UCProxyViewSupport
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
    // Per the RelationCatalog contract, listTables must return tables only -- view-like UC
    // table types (metric views, materialized views) belong on listViews.
    listUCTableLikes(namespace)
      .filterNot(t => UCViewTypes.isViewLikeTableType(t.getTableType))
      .map(table => Identifier.of(namespace, table.getName))
      .toArray
  }

  private[spark] def listUCTableLikes(namespace: Array[String]): Seq[UCTableInfo] = {
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

  // Returns `None` on a 404 so callers can branch on presence/absence without catching an
  // exception. `loadTable` re-raises `NoSuchTableException` on `None` (Spark's resolver expects
  // that); the passive-filter paths (`dropTable`, `dropView`) and `loadView` map `None` to their
  // own not-found result.
  private[spark] def getUCTableLike(ident: Identifier): Option[UCTableInfo] = {
    val fullName = UCSingleCatalog.fullTableNameForApi(this.name, ident)
    try {
      Some(tablesApi.getTable(
        fullName,
        /* readStreamingTableAsManaged = */ true,
        /* readMaterializedViewAsManaged = */ true))
    } catch {
      case e: ApiException if e.getCode == 404 => None
    }
  }

  // Single-RPC table path. Calls UC `getTable` once and rejects view-like rows from the
  // table-only surface by throwing `NoSuchTableException`.
  override def loadTable(ident: Identifier): Table = {
    val t = getUCTableLike(ident).getOrElse(throw new NoSuchTableException(ident))
    if (UCViewTypes.isViewLikeTableType(t.getTableType)) {
      throw new ViewFoundDuringTableLoadException(ident, t)
    } else {
      loadV1Table(t)
    }
  }

  private[spark] def loadV1Table(t: UCTableInfo): Table = {
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

    val credBuilder =
      UCCredentialHadoopConfs
        .builder(uri.toString, locationUri.getScheme)
        .addAppVersions(ApiClientFactory.appEngineVersions())
        .tokenProvider(tokenProvider)
        .apiClient(apiClient)
        .enableCredentialRenewal(renewCredEnabled)
        .enableCredentialScopedFs(credScopedFsEnabled)
        .hadoopConf(UCSingleCatalog.sessionHadoopConf())

    // TODO: at this time, we don't know if the table will be read or written. For now we always
    //       request READ_WRITE credentials as the server doesn't distinguish between READ and
    //       READ_WRITE credentials as of today. When loading a table, Spark should tell if it's
    //       for read or write, we can request the proper credential after fixing Spark.
    val extraSerdeProps = try {
      credBuilder.buildForTable(tableId, TableOperation.READ_WRITE)
    } catch {
      case e: ApiException =>
        logWarning(s"READ_WRITE credential generation failed for table $identifier: ${e.getMessage}")
        try {
          credBuilder.buildForTable(tableId, TableOperation.READ)
        } catch {
          case e: ApiException =>
            logWarning(s"READ credential generation failed for table $identifier: ${e.getMessage}")
            if (serverSidePlanningEnabled) {
              enableServerSidePlanningConfig(identifier)
              Map.empty[String, String].asJava
            } else {
              throw e
            }
        }
    }

    // For unrecognized schemes (e.g. file://) the credential switch returns empty without props;
    // treat that the same as a failed fetch so SSP-enabled tables on unsupported schemes still work.
    if (extraSerdeProps.isEmpty && serverSidePlanningEnabled) {
      enableServerSidePlanningConfig(identifier)
    }

    val storageProperties = (t.getProperties.asScala.toMap ++ extraSerdeProps).asJava
    val sparkTable = CatalogTable(
      identifier,
      tableType = if (t.getTableType == TableType.MANAGED) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      },
      storage = SparkCatalogCompatibility.catalogStorageFormatWithLocation(
        locationUri, storageProperties),
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

  /**
   * Spark 4.2's `TableCatalog` made `createTable(ident, TableInfo)` the canonical entry
   * point; this legacy 4-arg method carries all the create-table logic, which keeps the diff
   * vs. main scoped to "add the new overload + 409 -> TableAlreadyExistsException catch required
   * by the relation catalog's active-rejection contract".
   */
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

    val partitionColNames: Seq[String] =
      UCColumnConversions.partitionColumnNames(partitions).asScala.toSeq
    val columns: Seq[ColumnInfo] = schema.fields.toSeq.zipWithIndex.map { case (field, i) =>
      val column = new ColumnInfo()
      column.setName(field.name)
      if (field.getComment().isDefined) {
        column.setComment(field.getComment.get)
      }
      column.setNullable(field.nullable)
      column.setTypeText(field.dataType.catalogString)
      column.setTypeName(convertDataTypeToTypeName(field.dataType))
      column.setTypeJson(UCColumnConversions.toStructFieldJson(field))
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
      // RelationCatalog's active-rejection contract: createTable must throw
      // TableAlreadyExistsException when a view (or another table, race) already sits at
      // `ident`. UC returns 409 in both cases.
      case e: ApiException if e.getCode == 409 =>
        throw new TableAlreadyExistsException(ident)
    }
    loadTable(ident)
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

  private[spark] def convertDataTypeToTypeName(dataType: DataType): ColumnTypeName = {
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
    // of object actually sits at `ident`. `RelationCatalog` puts tables and views in a
    // shared identifier namespace, and the spec requires `dropTable` to passive-filter
    // view-like rows -- a `DROP TABLE <view>` must look like "nothing here" and return false
    // rather than delete the view. (`dropView` is symmetric: returns false for a table.)
    // We resolve the kind via UC `getTable` first so a view-like row never reaches
    // `tablesApi.deleteTable`.
    val t = getUCTableLike(ident) match {
      case Some(t) => t
      case None => return false
    }
    if (UCViewTypes.isViewLikeTableType(t.getTableType)) {
      return false
    }
    // `deleteTable` returns the (empty) response body, not an HTTP status; a real failure throws
    // ApiException. Issue the delete for its side effect and report success.
    tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident))
    true
  }

  // UC OSS does not currently expose a rename endpoint for tables or views; stub until UC adds
  // a rename API. `UCProxyViewSupport.renameView` mirrors this stub.
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
    // Spark 4.2 removed the namespaceExists() pre-check from CreateNamespaceExec. It relies on the
    // catalog throwing NamespaceAlreadyExistsException for IF NOT EXISTS handling. On Spark 4.0/4.1
    // the pre-check prevents this path.
    try {
      schemasApi.createSchema(createSchema)
    } catch {
      case e: ApiException if (e.getCode == 400 || e.getCode == 409) &&
        e.getResponseBody != null &&
        e.getResponseBody.contains("SCHEMA_ALREADY_EXISTS") =>
        throw new NamespaceAlreadyExistsException(namespace)
    }
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
