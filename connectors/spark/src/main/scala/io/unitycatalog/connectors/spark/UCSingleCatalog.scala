package io.unitycatalog.connectors.spark

import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.client.api.{TablesApi, TemporaryTableCredentialsApi}
import io.unitycatalog.client.model.{AwsCredentials, GenerateTemporaryTableCredential, TableOperation, TableType}

import java.net.URI
import java.util
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters._

/**
 * A Spark catalog plugin to get/manage tables in Unity Catalog.
 */
class UCSingleCatalog extends TableCatalog {

  private var deltaCatalog: TableCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val proxy = new UCProxy()
    proxy.initialize(name, options)
    deltaCatalog = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
    deltaCatalog.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
  }

  override def name(): String = deltaCatalog.name()

  override def listTables(namespace: Array[String]): Array[Identifier] = ???

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

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???
}

// An internal proxy to talk to the UC client.
private class UCProxy extends TableCatalog {
  private[this] var name: String = null
  private[this] var tablesApi: TablesApi = null
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
    temporaryTableCredentialsApi = new TemporaryTableCredentialsApi(client);
  }

  override def name(): String = {
    assert(this.name != null)
    this.name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = ???

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

  override def createTable(ident: Identifier, columns: Array[Column], partitions: Array[Transform], properties: util.Map[String, String]): Table = ???

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    throw new AssertionError("deprecated `createTable` should not be called")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

}
