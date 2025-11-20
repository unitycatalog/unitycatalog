package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.TableInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.shaded.com.google.common.base.Joiner;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class UCProxy2 implements TableCatalog, SupportsNamespaces {
  private final ApiClient apiClient;
  private final TemporaryCredentialsApi temporaryCredentialsApi;

  private String name = null;
  private TablesApi tablesApi = null;
  private SchemasApi schemasApi = null;

  public UCProxy2(ApiClient apiClient, TemporaryCredentialsApi temporaryCredentialsApi) {
    this.apiClient = apiClient;
    this.temporaryCredentialsApi = temporaryCredentialsApi;
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.tablesApi = new TablesApi(apiClient);
    this.schemasApi = new SchemasApi(apiClient);
  }

  @Override
  public String name() {
    assert name != null;
    return name;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    try {
      checkUnsupportedNestedNamespace(namespace);

      String catalogName = name;
      String schemaName = namespace[0];
      int maxResults = 0;
      ListTablesResponse response = tablesApi.listTables(catalogName, schemaName, maxResults,
          null);

      return response.getTables()
          .stream()
          .map(table -> Identifier.of(namespace, table.getName()))
          .toArray(Identifier[]::new);
      // Is it correct to throw this exception ?
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    TableInfo t;
    try {
      t = tablesApi.getTable(name + "." + ident.toString());
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
    }

    // TODO
    return null;
  }

  @Override
  public Table createTable(Identifier ident, StructType schema,
                           Transform[] partitions, Map<String, String> properties) {
    return null;
  }

  private DataSourceFormat convertToDataSourceFormat(String format) throws ApiException {
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
        throw new ApiException("DataSourceFormat not supported: " + format);
    }
  }

  private ColumnTypeName convertDataTypeToTypeName(DataType dataType) throws ApiException {
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
      throw new ApiException("DataType not supported: " + dataType.simpleString());
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("Altering a table is not supported yet");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      checkUnsupportedNestedNamespace(ident.namespace());

      Object ret = tablesApi.deleteTable(String.format("%s.%s.%s", name, ident.namespace()[0],
          ident.name()));
      return (int) ret == 200;
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("Renaming a table is not supported yet");
  }

  private void checkUnsupportedNestedNamespace(String[] namespace) throws ApiException {
    if (namespace.length > 1) {
      throw new ApiException("Nested namespaces are not supported:  " +
          Joiner.on(".").join(namespace));
    }
  }

  @Override
  public String[][] listNamespaces() {
    try {
      return schemasApi.listSchemas(name, 0, null)
          .getSchemas()
          .stream()
          .map(schema -> new String[]{schema.getName()})
          .toArray(String[][]::new);
    } catch (ApiException e) {
      throw new RuntimeException(e);
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
    try {
      checkUnsupportedNestedNamespace(namespace);

      SchemaInfo schema = null;
      try {
        schema = schemasApi.getSchema(name + "." + namespace[0]);
      } catch (ApiException e) {
        if (e.getCode() == 404) {
          throw new NoSuchNamespaceException(namespace);
        }
      }

      Map<String, String> metadata = new HashMap<>();
      schema.getProperties().forEach((k, v) ->
          metadata.put(SchemaInfo.JSON_PROPERTY_PROPERTIES + ":" + k, v));

      metadata.put(SchemaInfo.JSON_PROPERTY_NAME, schema.getName());
      metadata.put(SchemaInfo.JSON_PROPERTY_CATALOG_NAME, schema.getCatalogName());
      metadata.put(SchemaInfo.JSON_PROPERTY_COMMENT, schema.getComment());
      metadata.put(SchemaInfo.JSON_PROPERTY_FULL_NAME, schema.getFullName());

      if (schema.getCreatedAt() != null) {
        metadata.put(SchemaInfo.JSON_PROPERTY_CREATED_AT, schema.getCreatedAt().toString());
      } else {
        metadata.put(SchemaInfo.JSON_PROPERTY_CREATED_AT, "null");
      }

      if (schema.getUpdatedAt() != null) {
        metadata.put(SchemaInfo.JSON_PROPERTY_UPDATED_AT, schema.getUpdatedAt().toString());
      } else {
        metadata.put(SchemaInfo.JSON_PROPERTY_UPDATED_AT, "null");
      }

      metadata.put(SchemaInfo.JSON_PROPERTY_SCHEMA_ID, schema.getSchemaId());
      return metadata;
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    try {
      checkUnsupportedNestedNamespace(namespace);

      CreateSchema createSchema = new CreateSchema();
      createSchema.setCatalogName(name);
      createSchema.setName(namespace[0]);
      createSchema.setProperties(metadata);

      schemasApi.createSchema(createSchema);
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw new UnsupportedOperationException("Renaming a namespace is not supported yet");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    try {
      checkUnsupportedNestedNamespace(namespace);
      schemasApi.deleteSchema(name + "." + namespace[0], cascade);

      return true;
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }
}
