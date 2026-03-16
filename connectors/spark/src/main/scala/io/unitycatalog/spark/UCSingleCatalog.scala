package io.unitycatalog.spark

import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{TableInfo, _}
import io.unitycatalog.client.retry.JitterDelayRetryPolicy
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.spark.auth.{AuthConfigUtils, CredPropsUtil}
import io.unitycatalog.spark.utils.OptionsUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.sparkproject.guava.base.Preconditions

import java.net.URI
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.language.existentials

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog
  extends StagingTableCatalog
  with SupportsNamespaces
  with Logging {

  private[this] var uri: URI = null
  private[this] var tokenProvider: TokenProvider = null
  private[this] var renewCredEnabled: Boolean = false
  private[this] var credScopedFsEnabled: Boolean = false
  private[this] var apiClient: ApiClient = null;
  private[this] var temporaryCredentialsApi: TemporaryCredentialsApi = null
  private[this] var tablesApi: TablesApi = null

  @volatile private var delegate: TableCatalog = null

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
    val credentialProps = CredPropsUtil.createTableCredProps(
      renewCredEnabled,
      credScopedFsEnabled,
      CatalogUtils.stringToURI(stagingLocation).getScheme,
      uri.toString,
      tokenProvider,
      stagingTableId,
      TableOperation.READ_WRITE,
      temporaryCredentials,
    )
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
    val newProps = new util.HashMap[String, String]
    newProps.putAll(properties)
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
    val credentialProps = CredPropsUtil.createTableCredProps(
      renewCredEnabled,
      tableUriScheme,
      uri.toString,
      tokenProvider,
      tableId,
      TableOperation.READ_WRITE,
      temporaryCredentials,
    )
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

    val credentialProps = CredPropsUtil.createPathCredProps(
      renewCredEnabled,
      credScopedFsEnabled,
      CatalogUtils.stringToURI(location).getScheme,
      uri.toString,
      tokenProvider,
      location,
      PathOperation.PATH_CREATE_TABLE,
      cred)

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
    temporaryCredentialsApi: TemporaryCredentialsApi) extends TableCatalog with SupportsNamespaces with Logging {
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

    val catalogName = this.name
    val schemaName = namespace.head
    val maxResults = 0
    val pageToken = null
    val response: ListTablesResponse = tablesApi.listTables(catalogName, schemaName, maxResults, pageToken)
    response.getTables.toSeq.map(table => Identifier.of(namespace, table.getName)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val t = try {
      tablesApi.getTable(
        UCSingleCatalog.fullTableNameForApi(this.name, ident),
        /* readStreamingTableAsManaged = */ true,
        /* readMaterializedViewAsManaged = */ true)
    } catch {
      case e: ApiException if e.getCode == 404 =>
        throw new NoSuchTableException(ident)
    }
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
      CredPropsUtil.createTableCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        locationUri.getScheme,
        uri.toString,
        tokenProvider,
        tableId,
        tableOp,
        temporaryCredentials,
      )
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

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    assert(properties.get(TableCatalog.PROP_PROVIDER) != null)

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
    tablesApi.createTable(createTable)
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
    val ret = tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident))
    if (ret == 200) true else false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  override def listNamespaces(): Array[Array[String]] = {
    schemasApi.listSchemas(name, 0, null).getSchemas.asScala.map { schema =>
      Array(schema.getName)
    }.toArray
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
