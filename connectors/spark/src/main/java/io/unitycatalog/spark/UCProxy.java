package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCSingleCatalog.checkUnsupportedNestedNamespace;
import static io.unitycatalog.spark.UCSingleCatalog.fullTableNameForApi;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.auth.CredPropsUtil;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

public class UCProxy implements TableCatalog, SupportsNamespaces {
  private final URI uri;
  private final TokenProvider tokenProvider;
  private final boolean renewCredEnabled;
  private final ApiClient apiClient;
  private final TablesApi tablesApi;
  private final TemporaryCredentialsApi temporaryCredentialsApi;
  private String name = null;
  private SchemasApi schemasApi = null;

  UCProxy(
      URI uri,
      TokenProvider tokenProvider,
      boolean renewCredEnabled,
      ApiClient apiClient,
      TablesApi tablesApi,
      TemporaryCredentialsApi temporaryCredentialsApi) {
    this.uri = uri;
    this.tokenProvider = tokenProvider;
    this.renewCredEnabled = renewCredEnabled;
    this.apiClient = apiClient;
    this.tablesApi = tablesApi;
    this.temporaryCredentialsApi = temporaryCredentialsApi;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    schemasApi = new SchemasApi(apiClient);
  }

  @Override
  public String name() {
    assert this.name != null;
    return this.name;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    checkUnsupportedNestedNamespace(namespace);

    String catalogName = this.name;
    String schemaName = namespace[0];
    int maxResults = 0;
    String pageToken = null;
    ListTablesResponse response;
    try {
      response = tablesApi.listTables(catalogName, schemaName, maxResults, pageToken);
    } catch (ApiException e) {
      throw new RuntimeException("Failed to list tables", e);
    }
    return response.getTables().stream()
        .map(table -> Identifier.of(namespace, table.getName()))
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    TableInfo t;
    try {
      t =
          tablesApi.getTable(
              fullTableNameForApi(this.name, ident),
              /* readStreamingTableAsManaged = */ true,
              /* readMaterializedViewAsManaged = */ true);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw new RuntimeException("Failed to get table", e);
    }
    TableIdentifier identifier =
        new TableIdentifier(
            t.getName(), Option.apply(t.getSchemaName()), Option.apply(t.getCatalogName()));
    Map<String, Integer> partitionCols = new HashMap<>();
    List<StructField> fields = new ArrayList<>();
    for (ColumnInfo col : t.getColumns()) {
      Integer index = col.getPartitionIndex();
      if (index != null) {
        partitionCols.put(col.getName(), index);
      }
      StructField field =
          new StructField(
              col.getName(),
              DataType.fromDDL(col.getTypeText()),
              col.getNullable(),
              Metadata.empty());
      if (col.getComment() != null) {
        field = field.withComment(col.getComment());
      }
      fields.add(field);
    }
    URI locationUri = CatalogUtils.stringToURI(t.getStorageLocation());
    String tableId = t.getTableId();
    TableOperation tableOp = TableOperation.READ_WRITE;
    TemporaryCredentials temporaryCredentials;
    try {
      temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryTableCredentials(
              // TODO: at this time, we don't know if the table will be read or
              // written. For now we always request READ_WRITE credentials as the
              // server doesn't distinguish between READ and READ_WRITE credentials
              // as of today. When loading a table, Spark should tell if it's for
              // read or write, we can request the proper credential after fixing Spark.
              new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp));
    } catch (ApiException e) {
      tableOp = TableOperation.READ;
      try {
        temporaryCredentials =
            temporaryCredentialsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp));
      } catch (ApiException ex) {
        throw new RuntimeException("Failed to generate temporary credentials", ex);
      }
    }

    Map<String, String> extraSerdeProps =
        CredPropsUtil.createTableCredProps(
            renewCredEnabled,
            locationUri.getScheme(),
            uri.toString(),
            tokenProvider,
            tableId,
            tableOp,
            temporaryCredentials);

    // Convert partition columns map to sorted list
    List<String> partitionColumnNames =
        partitionCols.entrySet().stream()
            .sorted(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    // Merge properties
    Map<String, String> storageProperties = new HashMap<>();
    if (t.getProperties() != null) {
      storageProperties.putAll(t.getProperties());
    }
    storageProperties.putAll(extraSerdeProps);

    scala.collection.immutable.Map<String, String> storageProps =
        scala.collection.immutable.Map$.MODULE$.from(
            CollectionConverters.asScala(storageProperties));
    scala.collection.immutable.Map<String, String> emptyMap =
        scala.collection.immutable.Map$.MODULE$.from(
            CollectionConverters.asScala(new HashMap<String, String>()));
    scala.collection.immutable.Seq<String> partitionColSeq =
        CollectionConverters.asScala(partitionColumnNames).toSeq();
    scala.collection.immutable.Seq<String> emptySeq =
        CollectionConverters.asScala(new ArrayList<String>()).toSeq();

    CatalogTable sparkTable =
        new CatalogTable(
            identifier,
            t.getTableType() == TableType.MANAGED
                ? CatalogTableType.MANAGED()
                : CatalogTableType.EXTERNAL(),
            CatalogStorageFormat.empty()
                .copy(
                    Option.apply(locationUri),
                    Option.empty(),
                    Option.empty(),
                    Option.empty(),
                    false,
                    storageProps),
            new StructType(fields.toArray(new StructField[0])),
            Option.apply(t.getDataSourceFormat().getValue().toLowerCase()),
            partitionColSeq,
            Option.empty(),
            "", // owner
            t.getCreatedAt(),
            0L, // lastAccessTime
            "", // createVersion
            emptyMap, // properties
            Option.empty(), // stats
            Option.empty(), // viewText
            Option.empty(), // comment
            Option.empty(), // unsupportedFeatures
            emptySeq, // clusterByNames
            false, // tracksPartitionsInCatalog
            false, // schemaPreservesCase
            emptyMap, // serdeProperties
            Option.empty());

    // Spark separates table lookup and data source resolution. To support Spark native data
    // sources, here we return the `V1Table` which only contains the table metadata. Spark will
    // resolve the data source and create scan node later.
    try {
      return (Table)
          Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
              .getDeclaredConstructor(CatalogTable.class)
              .newInstance(sparkTable);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create V1Table", e);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    checkUnsupportedNestedNamespace(ident.namespace());
    assert properties.get(TableCatalog.PROP_PROVIDER) != null;

    CreateTable createTable = new CreateTable();
    createTable.setName(ident.name());
    createTable.setSchemaName(ident.namespace()[0]);
    createTable.setCatalogName(this.name);

    boolean hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL);
    String storageLocation = properties.get(TableCatalog.PROP_LOCATION);
    assert storageLocation != null
        : "location should either be user specified or system generated.";
    String isManagedLocationStr = properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION);
    boolean isManagedLocation =
        isManagedLocationStr != null && isManagedLocationStr.equalsIgnoreCase("true");
    String format = properties.get("provider");
    if (isManagedLocation) {
      assert !hasExternalClause : "location is only generated for managed tables.";
      if (!format.equalsIgnoreCase(DataSourceFormat.DELTA.name())) {
        throw new RuntimeException("Unity Catalog does not support non-Delta managed table.");
      }
      createTable.setTableType(TableType.MANAGED);
    } else {
      createTable.setTableType(TableType.EXTERNAL);
    }
    createTable.setStorageLocation(storageLocation);

    List<ColumnInfo> columns =
        IntStream.range(0, schema.fields().length)
            .mapToObj(
                i -> {
                  StructField field = schema.fields()[i];
                  ColumnInfo column = new ColumnInfo();
                  column.setName(field.name());
                  if (field.getComment().isDefined()) {
                    column.setComment(field.getComment().get());
                  }
                  column.setNullable(field.nullable());
                  column.setTypeText(field.dataType().simpleString());
                  column.setTypeName(convertDataTypeToTypeName(field.dataType()));
                  column.setTypeJson(field.dataType().json());
                  column.setPosition(i);
                  return column;
                })
            .collect(Collectors.toList());

    String comment = properties.get(TableCatalog.PROP_COMMENT);
    if (comment != null) {
      createTable.setComment(comment);
    }
    createTable.setColumns(columns);
    createTable.setDataSourceFormat(convertDatasourceFormat(format));
    // Do not send the V2 table properties as they are made part of the
    // `createTable` already.
    Map<String, String> propertiesToServer =
        properties.entrySet().stream()
            .filter(e -> !UCTableProperties.V2_TABLE_PROPERTIES.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    createTable.setProperties(propertiesToServer);
    try {
      tablesApi.createTable(createTable);
    } catch (ApiException e) {
      throw new RuntimeException("Failed to create table", e);
    }
    try {
      return loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to load created table", e);
    }
  }

  private DataSourceFormat convertDatasourceFormat(String format) {
    switch (format.toUpperCase()) {
      case "PARQUET":
        return DataSourceFormat.PARQUET;
      case "CSV":
        return DataSourceFormat.CSV;
      case "DELTA":
        return DataSourceFormat.DELTA;
      case "JSON":
        return DataSourceFormat.JSON;
      case "ORC":
        return DataSourceFormat.ORC;
      case "TEXT":
        return DataSourceFormat.TEXT;
      case "AVRO":
        return DataSourceFormat.AVRO;
      default:
        throw new RuntimeException("DataSourceFormat not supported: " + format);
    }
  }

  private ColumnTypeName convertDataTypeToTypeName(DataType dataType) {
    if (dataType instanceof StringType) {
      return ColumnTypeName.STRING;
    } else if (dataType instanceof BooleanType) {
      return ColumnTypeName.BOOLEAN;
    } else if (dataType instanceof ShortType) {
      return ColumnTypeName.SHORT;
    } else if (dataType instanceof IntegerType) {
      return ColumnTypeName.INT;
    } else if (dataType instanceof LongType) {
      return ColumnTypeName.LONG;
    } else if (dataType instanceof FloatType) {
      return ColumnTypeName.FLOAT;
    } else if (dataType instanceof DoubleType) {
      return ColumnTypeName.DOUBLE;
    } else if (dataType instanceof ByteType) {
      return ColumnTypeName.BYTE;
    } else if (dataType instanceof BinaryType) {
      return ColumnTypeName.BINARY;
    } else if (dataType instanceof TimestampNTZType) {
      return ColumnTypeName.TIMESTAMP_NTZ;
    } else if (dataType instanceof TimestampType) {
      return ColumnTypeName.TIMESTAMP;
    } else {
      throw new RuntimeException("DataType not supported: " + dataType.simpleString());
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("Altering a table is not supported yet");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    Object ret;
    try {
      ret = tablesApi.deleteTable(fullTableNameForApi(this.name, ident));
    } catch (ApiException e) {
      throw new RuntimeException("Failed to drop table", e);
    }
    return ret != null && ret.equals(200);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("Renaming a table is not supported yet");
  }

  @Override
  public String[][] listNamespaces() {
    try {
      return schemasApi.listSchemas(name, 0, null).getSchemas().stream()
          .map(schema -> new String[] {schema.getName()})
          .toArray(String[][]::new);
    } catch (ApiException e) {
      throw new RuntimeException("Failed to list namespaces", e);
    }
  }

  @Override
  public String[][] listNamespaces(String[] namespace) {
    throw new UnsupportedOperationException(
        "Multi-layer namespace is not supported in Unity Catalog");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    checkUnsupportedNestedNamespace(namespace);
    SchemaInfo schema;
    try {
      schema = schemasApi.getSchema(name + "." + namespace[0]);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchNamespaceException(namespace);
      }
      throw new RuntimeException("Failed to load namespace metadata", e);
    }
    // flatten the schema properties to a map, with the key prefixed by
    // "properties:"
    Map<String, String> metadata = new HashMap<>();
    if (schema.getProperties() != null) {
      schema
          .getProperties()
          .forEach((k, v) -> metadata.put(SchemaInfo.JSON_PROPERTY_PROPERTIES + ":" + k, v));
    }
    metadata.put(SchemaInfo.JSON_PROPERTY_NAME, schema.getName());
    metadata.put(SchemaInfo.JSON_PROPERTY_CATALOG_NAME, schema.getCatalogName());
    metadata.put(SchemaInfo.JSON_PROPERTY_COMMENT, schema.getComment());
    metadata.put(SchemaInfo.JSON_PROPERTY_FULL_NAME, schema.getFullName());
    metadata.put(
        SchemaInfo.JSON_PROPERTY_CREATED_AT,
        schema.getCreatedAt() != null ? schema.getCreatedAt().toString() : "null");
    metadata.put(
        SchemaInfo.JSON_PROPERTY_UPDATED_AT,
        schema.getUpdatedAt() != null ? schema.getUpdatedAt().toString() : "null");
    metadata.put(SchemaInfo.JSON_PROPERTY_SCHEMA_ID, schema.getSchemaId());
    return metadata;
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    checkUnsupportedNestedNamespace(namespace);
    CreateSchema createSchema = new CreateSchema();
    createSchema.setCatalogName(this.name);
    createSchema.setName(namespace[0]);
    createSchema.setProperties(metadata);
    try {
      schemasApi.createSchema(createSchema);
    } catch (ApiException e) {
      throw new RuntimeException("Failed to create namespace", e);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw new UnsupportedOperationException("Renaming a namespace is not supported yet");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    checkUnsupportedNestedNamespace(namespace);
    try {
      schemasApi.deleteSchema(name + "." + namespace[0], cascade);
    } catch (ApiException e) {
      throw new RuntimeException("Failed to drop namespace", e);
    }
    return true;
  }
}
