package io.unitycatalog.spark

import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.model.{ColumnInfo, ColumnTypeName, CreateSchema, CreateTable, DataSourceFormat, GenerateTemporaryPathCredential, GenerateTemporaryTableCredential, ListTablesResponse, PathOperation, SchemaInfo, TableOperation, TableType, TemporaryCredentials}

import java.net.URI
import java.util
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.{FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, FS_AZURE_ACCOUNT_IS_HNS_ENABLED, FS_AZURE_SAS_TOKEN_PROVIDER_TYPE}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions._
import scala.collection.JavaConverters._
import scala.language.existentials

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog
  extends TableCatalog
  with SupportsNamespaces
  with Logging {

  private[this] var url: URI = null
  private[this] var token: String = null
  private[this] var apiClient: ApiClient = null
  private[this] var temporaryCredentialsApi: TemporaryCredentialsApi = null

  @volatile private var delegate: TableCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize the url
    url = OptionsUtil.getUrl(options)

    // Initialize the token.
    token = OptionsUtil.getToken(options)

    // Initialize the api client.
    apiClient = ApiClientFactory.createApiClient(url, token)

    // Initialize the temporary credentials api.
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient)

    // Initialize the UC proxy.
    val proxy = new UCProxy(apiClient, temporaryCredentialsApi)
    proxy.initialize(name, options)

    // Initialize the delegate catalog.
    if (UCSingleCatalog.LOAD_DELTA_CATALOG.get()) {
      try {
        delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
        delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
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
    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val hasLocationClause = properties.containsKey(TableCatalog.PROP_LOCATION)
    if (hasExternalClause && !hasLocationClause) {
      throw new ApiException("Cannot create EXTERNAL TABLE without location.")
    }
    def isPathTable = ident.namespace().length == 1 && new Path(ident.name()).isAbsolute
    // If both EXTERNAL and LOCATION are not specified in the CREATE TABLE command, and the table is
    // not a path table like parquet.`/file/path`, we generate the UC-managed table location here.
    if (!hasExternalClause && !hasLocationClause && !isPathTable) {
      val newProps = new util.HashMap[String, String]
      newProps.putAll(properties)
      // TODO: here we use a fake location for managed table, we should generate table location
      //       properly when Unity Catalog supports creating managed table.
      newProps.put(TableCatalog.PROP_LOCATION, properties.get("__FAKE_PATH__"))
      // `PROP_IS_MANAGED_LOCATION` is used to indicate that the table location is not
      // user-specified but system-generated, which is exactly the case here.
      newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
      delegate.createTable(ident, columns, partitions, newProps)
    } else if (hasLocationClause) {
      val location = properties.get(TableCatalog.PROP_LOCATION)
      assert(location != null)

      // It's a per-table temporary path credentials.
      val cred = temporaryCredentialsApi.generateTemporaryPathCredentials(
        new GenerateTemporaryPathCredential().url(location).operation(PathOperation.PATH_CREATE_TABLE))

      // Add the table properties.
      val newProps = new util.HashMap[String, String]
      newProps.putAll(properties)

      // Add the object storage credential info.
      val credentialProps = UCSingleCatalog.createPathBasedCredProps(
        CatalogUtils.stringToURI(location).getScheme,
        url.toString,
        token,
        location,
        PathOperation.PATH_CREATE_TABLE,
        cred)
      newProps.putAll(credentialProps.asJava)

      // TODO: Delta requires the options to be set twice in the properties, with and without the
      //       `option.` prefix. We should revisit this in Delta.
      val prefix = TableCatalog.OPTION_PREFIX
      newProps.putAll(credentialProps.map {
        case (k, v) => (prefix + k, v)
      }.asJava)
      delegate.createTable(ident, columns, partitions, newProps)
    } else {
      // TODO: for path-based tables, Spark should generate a location property using the qualified
      //       path string.
      delegate.createTable(ident, columns, partitions, properties)
    }
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
}

object UCSingleCatalog {
  val LOAD_DELTA_CATALOG = ThreadLocal.withInitial[Boolean](() => true)
  val DELTA_CATALOG_LOADED = ThreadLocal.withInitial[Boolean](() => false)

  private def createPathBasedCredProps(scheme: String, unityCatalogURL: String, unityCatalogToken: String, path: String, pathOp: PathOperation, tempCred: TemporaryCredentials): Map[String, String] = {
    val props = commonProps(scheme, unityCatalogURL, unityCatalogToken, tempCred)

    // Add keys for the path based temporary credential requests.
    props ++ Map(
      Constants.UNITY_CATALOG_PATH -> path,
      Constants.UNITY_CATALOG_PATH_OPERATION -> pathOp.getValue,
      Constants.UNITY_CATALOG_CREDENTIALS_TYPE -> Constants.UNITY_CATALOG_PATH_CREDENTIALS_TYPE
    )
  }

  def createTableBasedCredProps(scheme: String, unityCatalogURL: String, unityCatalogToken: String, table: String, tableOp: TableOperation, tempCred: TemporaryCredentials): Map[String, String] = {
    val props = commonProps(scheme, unityCatalogURL, unityCatalogToken, tempCred)

    // Add keys for the path based temporary credential requests.
    props ++ Map(
      Constants.UNITY_CATALOG_TABLE -> table,
      Constants.UNITY_CATALOG_TABLE_OPERATION -> tableOp.getValue,
      Constants.UNITY_CATALOG_CREDENTIALS_TYPE -> Constants.UNITY_CATALOG_TABLE_CREDENTIALS_TYPE
    )
  }

  private def commonProps(scheme: String, unityCatalogURL: String, unityCatalogToken: String, tempCred: TemporaryCredentials): Map[String, String] = {
    if (scheme == "s3") {
      val awsCredentials = tempCred.getAwsTempCredentials
      Map(
        "fs.s3a.access.key" -> awsCredentials.getAccessKeyId,
        "fs.s3a.secret.key" -> awsCredentials.getSecretAccessKey,
        "fs.s3a.session.token" -> awsCredentials.getSessionToken,
        Constants.UNITY_CATALOG_URI -> unityCatalogURL,
        Constants.UNITY_CATALOG_TOKEN -> unityCatalogToken,
        "fs.s3a.aws.credentials.provider" -> classOf[AwsVendedTokenProvider].getName,
        "fs.s3a.path.style.access" -> "true",
        "fs.s3.impl.disable.cache" -> "true",
        "fs.s3a.impl.disable.cache" -> "true"
      )
    } else if (scheme == "gs") {
      val gcsCredentials = tempCred.getGcpOauthToken
      Map(
        GcsVendedTokenProvider.ACCESS_TOKEN_KEY -> gcsCredentials.getOauthToken,
        GcsVendedTokenProvider.ACCESS_TOKEN_EXPIRATION_KEY -> tempCred.getExpirationTime.toString,
        "fs.gs.create.items.conflict.check.enable" -> "false",
        "fs.gs.auth.type" -> "ACCESS_TOKEN_PROVIDER",
        "fs.gs.auth.access.token.provider" -> classOf[GcsVendedTokenProvider].getName,
        "fs.gs.impl.disable.cache" -> "true"
      )
    } else if (scheme == "abfs" || scheme == "abfss") {
      val azCredentials = tempCred.getAzureUserDelegationSas
      Map(
        FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME -> "SAS",
        FS_AZURE_ACCOUNT_IS_HNS_ENABLED -> "true",
        FS_AZURE_SAS_TOKEN_PROVIDER_TYPE -> classOf[AbfsVendedTokenProvider].getName,
        AbfsVendedTokenProvider.ACCESS_TOKEN_KEY -> azCredentials.getSasToken,
        "fs.abfs.impl.disable.cache" -> "true",
        "fs.abfss.impl.disable.cache" -> "true"
      )
    } else {
      Map.empty
    }
  }
}

// An internal proxy to talk to the UC client.
private class UCProxy(
    apiClient: ApiClient,
    temporaryCredentialsApi: TemporaryCredentialsApi) extends TableCatalog with SupportsNamespaces {
  private[this] var name: String = null
  private[this] var url: URI = null
  private[this] var token: String = null
  private[this] var tablesApi: TablesApi = null
  private[this] var schemasApi: SchemasApi = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.name = name

    // Initialize the url
    this.url = OptionsUtil.getUrl(options)

    // Initialize the token
    this.token = OptionsUtil.getToken(options)

    tablesApi = new TablesApi(apiClient)
    schemasApi = new SchemasApi(apiClient)
  }

  override def name(): String = {
    assert(this.name != null)
    this.name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    checkUnsupportedNestedNamespace(namespace)

    val catalogName = this.name
    val schemaName = namespace.head
    val maxResults = 0
    val pageToken = null
    val response: ListTablesResponse = tablesApi.listTables(catalogName, schemaName, maxResults, pageToken)
    response.getTables.toSeq.map(table => Identifier.of(namespace, table.getName)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val t = try {
      tablesApi.getTable(name + "." + ident.toString)
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
    val uri = CatalogUtils.stringToURI(t.getStorageLocation)
    val tableId = t.getTableId
    val temporaryCredentials = {
      try {
        temporaryCredentialsApi
          .generateTemporaryTableCredentials(
            // TODO: at this time, we don't know if the table will be read or written. For now we always
            //       request READ_WRITE credentials as the server doesn't distinguish between READ and
            //       READ_WRITE credentials as of today. When loading a table, Spark should tell if it's
            //       for read or write, we can request the proper credential after fixing Spark.
            new GenerateTemporaryTableCredential().tableId(tableId).operation(TableOperation.READ_WRITE)
          )
      } catch {
        case e: ApiException => temporaryCredentialsApi
          .generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential().tableId(tableId).operation(TableOperation.READ)
          )
      }
    }
    val extraSerdeProps = UCSingleCatalog.createTableBasedCredProps(
      uri.getScheme,
      url.toString,
      token,
      tableId,
      TableOperation.READ_WRITE,
      temporaryCredentials)
    val sparkTable = CatalogTable(
      identifier,
      tableType = if (t.getTableType == TableType.MANAGED) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      },
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(uri),
        properties = t.getProperties.asScala.toMap ++ extraSerdeProps
      ),
      schema = StructType(fields),
      provider = Some(t.getDataSourceFormat.getValue),
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
    checkUnsupportedNestedNamespace(ident.namespace())
    assert(properties.get("provider") != null)

    val createTable = new CreateTable()
    createTable.setName(ident.name())
    createTable.setSchemaName(ident.namespace().head)
    createTable.setCatalogName(this.name)

    val hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL)
    val storageLocation = properties.get(TableCatalog.PROP_LOCATION)
    assert(storageLocation != null, "location should either be user specified or system generated.")
    val isManagedLocation = Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
      .exists(_.equalsIgnoreCase("true"))
    if (isManagedLocation) {
      assert(!hasExternalClause, "location is only generated for managed tables.")
      // TODO: Unity Catalog does not support managed tables now.
      throw new ApiException("Unity Catalog does not support managed table.")
    }
    createTable.setTableType(TableType.EXTERNAL)
    createTable.setStorageLocation(storageLocation)

    val columns: Seq[ColumnInfo] = schema.fields.toSeq.zipWithIndex.map { case (field, i) =>
      val column = new ColumnInfo()
      column.setName(field.name)
      if (field.getComment().isDefined) {
        column.setComment(field.getComment.get)
      }
      column.setNullable(field.nullable)
      column.setTypeText(field.dataType.simpleString)
      column.setTypeName(convertDataTypeToTypeName(field.dataType))
      column.setTypeJson(field.dataType.json)
      column.setPosition(i)
      column
    }
    createTable.setColumns(columns)
    val format: String = properties.get("provider")
    createTable.setDataSourceFormat(convertDatasourceFormat(format))
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
      case StringType => ColumnTypeName.STRING
      case BooleanType => ColumnTypeName.BOOLEAN
      case ShortType => ColumnTypeName.SHORT
      case IntegerType => ColumnTypeName.INT
      case LongType => ColumnTypeName.LONG
      case FloatType => ColumnTypeName.FLOAT
      case DoubleType => ColumnTypeName.DOUBLE
      case ByteType => ColumnTypeName.BYTE
      case BinaryType => ColumnTypeName.BINARY
      case TimestampNTZType => ColumnTypeName.TIMESTAMP_NTZ
      case TimestampType => ColumnTypeName.TIMESTAMP
      case _ => throw new ApiException("DataType not supported: " + dataType.simpleString)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering a table is not supported yet")
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkUnsupportedNestedNamespace(ident.namespace())
    val ret =
      tablesApi.deleteTable(Seq(this.name, ident.namespace()(0), ident.name()).mkString("."))
    if (ret == 200) true else false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a table is not supported yet")
  }

  private def checkUnsupportedNestedNamespace(namespace: Array[String]): Unit = {
    if (namespace.length > 1) {
      throw new ApiException("Nested namespaces are not supported:  " + namespace.mkString("."))
    }
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
    checkUnsupportedNestedNamespace(namespace)
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
    checkUnsupportedNestedNamespace(namespace)
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
    checkUnsupportedNestedNamespace(namespace)
    schemasApi.deleteSchema(name + "." + namespace.head, cascade)
    true
  }
}

object OptionsUtil {
  def getUrl(options: CaseInsensitiveStringMap): URI = {
    val urlStr = options.get("uri")
    if (urlStr == null) {
      throw new IllegalArgumentException(s"uri must be specified in Unity Catalog options")
    }
    new URI(urlStr)
  }

  def getToken(options: CaseInsensitiveStringMap): String = {
    options.get("token")
  }
}

