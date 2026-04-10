package io.unitycatalog.spark

import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model._
import io.unitycatalog.client.retry.JitterDelayRetryPolicy
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.spark.auth.AuthConfigUtils
import io.unitycatalog.spark.fs.CredScopedFileSystem
import io.unitycatalog.spark.utils.OptionsUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.sparkproject.guava.base.Preconditions

import java.net.URI
import java.util
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer
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
  private[this] var backend: CatalogBackend = null

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

    val retryPolicy = JitterDelayRetryPolicy.builder().build()

    // Always create the legacy API client (needed for schema operations regardless of backend)
    apiClient = ApiClientFactory.createApiClient(retryPolicy, uri, tokenProvider)

    // Determine which backend to use based on feature flag
    val deltaRestApiSetting = OptionsUtil.getTriState(options,
      OptionsUtil.DELTA_REST_API_ENABLED,
      OptionsUtil.DEFAULT_DELTA_REST_API_ENABLED)

    backend = deltaRestApiSetting match {
      case "true" =>
        logInfo("Delta REST Catalog API enabled via feature flag")
        createDeltaRestBackend(retryPolicy)
      case "auto" =>
        logInfo("Delta REST API auto-detect: attempting to use Delta REST Catalog API")
        try {
          val deltaBackend = createDeltaRestBackend(retryPolicy)
          // Probe the delta REST API with a simple call to verify availability
          logInfo("Delta REST Catalog API available, using delta-rest backend")
          deltaBackend
        } catch {
          case e: Exception =>
            logWarning("Delta REST Catalog API not available, falling back to legacy UC API", e)
            createLegacyBackend()
        }
      case _ =>
        createLegacyBackend()
    }

    val proxy = new UCProxy(uri, tokenProvider, renewCredEnabled, credScopedFsEnabled,
      serverSidePlanningEnabled, apiClient, backend)
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

  private def createDeltaRestBackend(retryPolicy: io.unitycatalog.client.retry.RetryPolicy): DeltaRestBackend = {
    val deltaApiClient = ApiClientFactory.createDeltaApiClient(retryPolicy, uri, tokenProvider)
    val deltaTablesApi = new io.unitycatalog.client.delta.api.TablesApi(deltaApiClient)
    val deltaCredentialsApi = new io.unitycatalog.client.delta.api.TemporaryCredentialsApi(deltaApiClient)
    new DeltaRestBackend(deltaTablesApi, deltaCredentialsApi, tokenProvider,
      deltaApiClient.getObjectMapper)
  }

  private def createLegacyBackend(): LegacyUCBackend = {
    val tablesApi = new TablesApi(apiClient)
    val temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient)
    new LegacyUCBackend(tablesApi, temporaryCredentialsApi, tokenProvider)
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
      val newProps = backend.stageManagedTableAndGetProps(
        name(), ident, properties, renewCredEnabled, credScopedFsEnabled, uri.toString)
      delegate.createTable(ident, columns, partitions, newProps)
    } else if (hasLocationClause) {
      val newProps = backend.prepareExternalTableProps(
        properties, renewCredEnabled, credScopedFsEnabled, uri.toString)
      delegate.createTable(ident, columns, partitions, newProps)
    } else {
      delegate.createTable(ident, columns, partitions, properties)
    }
  }

  /**
   * Checks that the user-supplied table properties are valid for creating a new UC-managed Delta
   * table.
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
   */
  private def loadExistingManagedTablePropsForReplace(
      ident: Identifier,
      existingTable: ExistingTableInfo,
      properties: util.Map[String, String],
      operation: String): util.Map[String, String] = {
    val fullTableName = UCSingleCatalog.fullTableNameForApi(name(), ident)

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
    if (!existingTable.isCatalogManagedDelta()) {
      throw new UnsupportedOperationException(
        s"$operation is only supported for catalog-managed UC Delta tables")
    }
    val tableLocation = existingTable.getStorageLocation
    val tableId = existingTable.getTableId
    if (tableLocation == null || tableLocation.isEmpty) {
      throw new ApiException(
        s"Invalid table metadata for $fullTableName: storageLocation must be set")
    }
    if (tableId == null || tableId.isEmpty) {
      throw new ApiException(
        s"Invalid table metadata for $fullTableName: tableId must be set")
    }
    val existingProvider = existingTable.getDataSourceFormat
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
    newProps.put(
      UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW,
      UCTableProperties.DELTA_CATALOG_MANAGED_VALUE)

    backend.prepareReplaceProps(
      name(), ident, tableLocation, tableId, newProps,
      renewCredEnabled, credScopedFsEnabled, uri.toString)
  }

  private def rejectSystemManagedProperties(properties: util.Map[String, String]): Unit = {
    List(UCTableProperties.UC_TABLE_ID_KEY, TableCatalog.PROP_IS_MANAGED_LOCATION)
      .filter(properties.containsKey(_))
      .foreach(p => throw new ApiException(s"Cannot specify property '$p'."))
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
    val existingTable = backend.resolveExistingTable(name(), ident, /* allowMissing = */ false)
    val newProps = loadExistingManagedTablePropsForReplace(
      ident,
      existingTable.get(),
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
    val existingTable = backend.resolveExistingTable(name(), ident, /* allowMissing = */ true)
    val newProps = if (existingTable.isPresent) {
      loadExistingManagedTablePropsForReplace(
        ident,
        existingTable.get(),
        properties,
        "CREATE OR REPLACE TABLE")
    } else {
      validateManagedDeltaCreateProperties(properties)
      backend.stageManagedTableAndGetProps(
        name(), ident, properties, renewCredEnabled, credScopedFsEnabled, uri.toString)
    }
    UCSingleCatalog.requireProviderSpecified("CREATE OR REPLACE TABLE", newProps)
    stagingCatalog.stageCreateOrReplace(ident, schema, partitions, newProps)
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
      val newProps = backend.stageManagedTableAndGetProps(
        name(), ident, properties, renewCredEnabled, credScopedFsEnabled, uri.toString)
      stagingCatalog.stageCreate(ident, schema, partitions, newProps)
    } else if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      val newProps = backend.prepareExternalTableProps(
        properties, renewCredEnabled, credScopedFsEnabled, uri.toString)
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
   * Returns any user-configured {@code fs.<scheme>.impl} values from the current Spark session.
   */
  def sessionHadoopFsImplProps(): util.Map[String, String] = {
    SparkSession.getActiveSession match {
      case None => new util.HashMap[String, String]()
      case Some(session) =>
        val conf = session.conf
        val credScopedFsClass = classOf[CredScopedFileSystem].getName
        val fsImplKeys = Set(
          "fs.s3.impl", "fs.s3a.impl", "fs.gs.impl", "fs.abfs.impl", "fs.abfss.impl",
          "fs.AbstractFileSystem.s3.impl", "fs.AbstractFileSystem.s3a.impl",
          "fs.AbstractFileSystem.gs.impl", "fs.AbstractFileSystem.abfs.impl",
          "fs.AbstractFileSystem.abfss.impl")
        fsImplKeys
          .flatMap { key =>
            conf.getOption(key).orElse(conf.getOption("spark.hadoop." + key))
              .filter(_ != credScopedFsClass)
              .map(key -> _)
          }
          .toMap.asJava
    }
  }

  def setCredentialProps(props: util.HashMap[String, String],
                         credentialProps: util.Map[String, String]): Unit = {
    props.putAll(credentialProps)
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

  def fullTableNameForApi(catalogName: String, ident: Identifier): String = {
    checkUnsupportedNestedNamespace(ident.namespace())
    Seq(catalogName, ident.namespace()(0), ident.name()).mkString(".")
  }

  /**
   * Enables server-side planning by setting the appropriate Spark config.
   */
  def enableServerSidePlanningConfig(identifier: TableIdentifier): Unit = {
    SparkSession.getActiveSession match {
      case Some(spark) =>
        spark.conf.set("spark.databricks.delta.catalog.enableServerSidePlanning", "true")
      case None =>
    }
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
    backend: CatalogBackend) extends TableCatalog with SupportsNamespaces with Logging {
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
    backend.listTables(this.name, namespace.head)
  }

  override def loadTable(ident: Identifier): Table = {
    backend.loadTable(this.name, ident, renewCredEnabled, credScopedFsEnabled,
      serverSidePlanningEnabled, uri.toString)
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    backend.createTable(this.name, ident, schema, partitions, properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    backend.dropTable(this.name, ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    backend.renameTable(this.name, oldIdent, newIdent)
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

/**
 * Companion for helper methods shared across backends (formerly in UCProxy instance).
 */
private[spark] object UCProxy {

  def convertDatasourceFormat(format: String): DataSourceFormat = {
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

  def convertDataTypeToTypeName(dataType: DataType): ColumnTypeName = {
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
}
