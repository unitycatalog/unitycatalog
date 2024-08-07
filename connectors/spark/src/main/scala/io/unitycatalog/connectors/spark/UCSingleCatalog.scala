package io.unitycatalog.connectors.spark

import com.google.gson.internal.bind.DefaultDateTypeAdapter.DateType
import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.client.api.{SchemasApi, TablesApi, TemporaryTableCredentialsApi}
import io.unitycatalog.client.model.{AwsCredentials, CreateSchema, GenerateTemporaryTableCredential, ListTablesResponse, SchemaInfo, TableOperation, TableType, UpdateSchema}
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp

import java.net.URI
import java.util
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.NamespaceChange.SetProperty
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions._
import scala.jdk.CollectionConverters._

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog extends TableCatalog with SupportsNamespaces {

  private var deltaCatalog: TableCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val proxy = new UCProxy()
    proxy.initialize(name, options)
    deltaCatalog = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
  }

  override def name(): String = deltaCatalog.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = deltaCatalog.listTables(namespace)

  override def loadTable(ident: Identifier): Table = deltaCatalog.loadTable(ident)

  override def tableExists(ident: Identifier): Boolean = {
    deltaCatalog.tableExists(ident)
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    deltaCatalog.createTable(ident, columns, partitions, properties)
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    throw new AssertionError("deprecated `createTable` should not be called")
  }
  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = deltaCatalog.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def listNamespaces(): Array[Array[String]] = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].loadNamespaceMetadata(namespace)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].dropNamespace(namespace, cascade)
  }
}

// An internal proxy to talk to the UC client.
private class UCProxy extends TableCatalog with SupportsNamespaces {
  private[this] var name: String = null
  private[this] var tablesApi: TablesApi = null
  private[this] var schemasApi: SchemasApi = null
  private[this] var temporaryTableCredentialsApi: TemporaryTableCredentialsApi = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.name = name
    val urlStr = options.get("uri")
    if (urlStr == null) {
      throw new IllegalArgumentException("uri must be specified for Unity Catalog")
    }
    val url = new URI(urlStr)
    var client = new ApiClient()
      .setHost(url.getHost)
      .setPort(url.getPort)
      .setScheme(url.getScheme)
    val token = options.get("token")
    if (token != null && token.nonEmpty) {
      client = client.setRequestInterceptor { request =>
        request.header("Authorization", "Bearer " + token)
      }
    }
    tablesApi = new TablesApi(client)
    temporaryTableCredentialsApi = new TemporaryTableCredentialsApi(client)
    schemasApi = new SchemasApi(client)
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
    val credential: AwsCredentials = temporaryTableCredentialsApi
      .generateTemporaryTableCredentials(
        // TODO: at this time, we don't know if the table will be read or written. We should get
        //       credential in a later phase.
        new GenerateTemporaryTableCredential().tableId(tableId).operation(TableOperation.READ_WRITE)
      )
      .getAwsTempCredentials
    val extraSerdeProps = if (uri.getScheme == "s3") {
      Map(
        // TODO: how to support s3:// properly?
        "fs.s3a.access.key" -> credential.getAccessKeyId,
        "fs.s3a.secret.key" -> credential.getSecretAccessKey,
        "fs.s3a.session.token" -> credential.getSessionToken,
        "fs.s3a.path.style.access" -> "true"
      )
    } else {
      Map.empty
    }
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
      .getDeclaredConstructor(classOf[CatalogTable]).newInstance(sparkTable)
      .asInstanceOf[Table]
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    checkUnsupportedNestedNamespace(ident.namespace())
    assert(properties.get(TableCatalog.PROP_EXTERNAL) == null)
    assert(properties.get("provider") != null)

    val createTable = new CreateTable()
    createTable.setName(ident.name())
    createTable.setSchemaName(ident.namespace().head)
    createTable.setCatalogName(this.name)
    val storageLocation = properties.get(TableCatalog.PROP_LOCATION)
    if (storageLocation == null) {
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
    null
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

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = {
    checkUnsupportedNestedNamespace(ident.namespace())
    val ret =
      tablesApi.deleteTable(Seq(this.name, ident.namespace()(0), ident.name()).mkString("."))
    if (ret == 200) true else false
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

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
    checkUnsupportedNestedNamespace(namespace)
    val updateSchema = new UpdateSchema()
    changes.foreach {
      case setProperty: SetProperty =>
        updateSchema.putPropertiesItem(setProperty.property(), setProperty.value())
      case p => throw new ApiException("UC does not support namespace change operation: " + p.toString)
    }
    schemasApi.updateSchema(name + "." + namespace.head, updateSchema)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    checkUnsupportedNestedNamespace(namespace)
    schemasApi.deleteSchema(name + "." + namespace.head, cascade)
    true
  }
}
