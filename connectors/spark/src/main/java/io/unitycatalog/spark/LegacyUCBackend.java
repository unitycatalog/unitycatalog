package io.unitycatalog.spark;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.spark.auth.CredPropsUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;

/**
 * {@link CatalogBackend} implementation using the original UC REST API ({@link TablesApi}, {@link
 * TemporaryCredentialsApi}).
 */
public class LegacyUCBackend implements CatalogBackend {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyUCBackend.class);

  private final TablesApi tablesApi;
  private final TemporaryCredentialsApi temporaryCredentialsApi;
  private final TokenProvider tokenProvider;

  public LegacyUCBackend(
      TablesApi tablesApi,
      TemporaryCredentialsApi temporaryCredentialsApi,
      TokenProvider tokenProvider) {
    this.tablesApi = tablesApi;
    this.temporaryCredentialsApi = temporaryCredentialsApi;
    this.tokenProvider = tokenProvider;
  }

  @Override
  public String apiPathLabel() {
    return "legacy-uc";
  }

  // -- Table CRUD --

  @Override
  public Identifier[] listTables(String catalogName, String schemaName) throws Exception {
    String[] namespace = new String[] {schemaName};
    List<Identifier> tables = new ArrayList<>();
    String pageToken = null;
    do {
      ListTablesResponse response = tablesApi.listTables(catalogName, schemaName, 0, pageToken);
      for (TableInfo t : response.getTables()) {
        tables.add(Identifier.of(namespace, t.getName()));
      }
      pageToken = response.getNextPageToken();
    } while (pageToken != null && !pageToken.isEmpty());
    return tables.toArray(new Identifier[0]);
  }

  @Override
  public Table loadTable(
      String catalogName,
      Identifier ident,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      boolean serverSidePlanningEnabled,
      String ucUri)
      throws Exception {
    TableInfo t;
    try {
      t = tablesApi.getTable(UCSingleCatalog.fullTableNameForApi(catalogName, ident), true, true);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw e;
    }

    TableIdentifier identifier =
        new TableIdentifier(
            t.getName(), new Some<>(t.getSchemaName()), new Some<>(t.getCatalogName()));

    List<int[]> partitionCols = new ArrayList<>();
    StructField[] fields = new StructField[t.getColumns().size()];
    for (int i = 0; i < t.getColumns().size(); i++) {
      ColumnInfo col = t.getColumns().get(i);
      if (col.getPartitionIndex() != null) {
        partitionCols.add(new int[] {col.getPartitionIndex(), i});
      }
      StructField field =
          new StructField(
              col.getName(),
              DataType.fromDDL(col.getTypeText()),
              col.getNullable(),
              org.apache.spark.sql.types.Metadata.empty());
      if (col.getComment() != null) {
        field = field.withComment(col.getComment());
      }
      fields[i] = field;
    }

    java.net.URI locationUri = CatalogUtils.stringToURI(t.getStorageLocation());
    String tableId = t.getTableId();

    TableOperation tableOp = TableOperation.READ_WRITE;
    TemporaryCredentials temporaryCredentials;
    try {
      temporaryCredentials =
          temporaryCredentialsApi.generateTemporaryTableCredentials(
              new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp));
    } catch (ApiException e) {
      LOG.warn(
          "READ_WRITE credential generation failed for table {}: {}", identifier, e.getMessage());
      try {
        tableOp = TableOperation.READ;
        temporaryCredentials =
            temporaryCredentialsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential().tableId(tableId).operation(tableOp));
      } catch (ApiException e2) {
        LOG.warn("READ credential generation failed for table {}: {}", identifier, e2.getMessage());
        if (serverSidePlanningEnabled) {
          temporaryCredentials = null;
        } else {
          throw e2;
        }
      }
    }

    if (serverSidePlanningEnabled && temporaryCredentials == null) {
      UCSingleCatalog.enableServerSidePlanningConfig(identifier);
    }

    Map<String, String> extraSerdeProps;
    if (temporaryCredentials == null) {
      extraSerdeProps = Collections.emptyMap();
    } else {
      extraSerdeProps =
          CredPropsUtil.createTableCredProps(
              renewCredEnabled,
              credScopedFsEnabled,
              UCSingleCatalog.sessionHadoopFsImplProps(),
              locationUri.getScheme(),
              ucUri,
              tokenProvider,
              tableId,
              tableOp,
              temporaryCredentials);
    }

    // Build property map
    Map<String, String> allProps = new HashMap<>();
    if (t.getProperties() != null) {
      allProps.putAll(t.getProperties());
    }
    allProps.putAll(extraSerdeProps);

    // Sort partition cols by partition index
    partitionCols.sort(Comparator.comparingInt(a -> a[0]));
    List<String> partColNames = new ArrayList<>();
    for (int[] pair : partitionCols) {
      partColNames.add(fields[pair[1]].name());
    }

    CatalogTable sparkTable =
        CatalogTableBuilder.build(
            identifier,
            t.getTableType() == TableType.MANAGED,
            locationUri,
            allProps,
            new StructType(fields),
            t.getDataSourceFormat().getValue().toLowerCase(),
            t.getCreatedAt(),
            partColNames);

    return CatalogTableBuilder.toV1Table(sparkTable);
  }

  @Override
  public Table createTable(
      String catalogName,
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties)
      throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    UCSingleCatalog.requireProviderSpecified("CREATE TABLE", properties);

    CreateTable createTable = new CreateTable();
    createTable.setName(ident.name());
    createTable.setSchemaName(ident.namespace()[0]);
    createTable.setCatalogName(catalogName);

    boolean hasExternalClause = properties.containsKey(TableCatalog.PROP_EXTERNAL);
    String storageLocation = properties.get(TableCatalog.PROP_LOCATION);
    assert storageLocation != null
        : "location should either be user specified or " + "system generated.";
    boolean isManagedLocation =
        "true".equalsIgnoreCase(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION));
    String format = properties.get("provider");
    if (isManagedLocation) {
      assert !hasExternalClause : "location is only generated for managed tables.";
      if (!format.equalsIgnoreCase(DataSourceFormat.DELTA.name())) {
        throw new ApiException("Unity Catalog does not support " + "non-Delta managed table.");
      }
      createTable.setTableType(TableType.MANAGED);
    } else {
      createTable.setTableType(TableType.EXTERNAL);
    }
    createTable.setStorageLocation(storageLocation);

    List<String> partitionColNames = new ArrayList<>();
    for (Transform t : partitions) {
      switch (t.name()) {
        case "identity":
          String[] fieldNames = t.references()[0].fieldNames();
          if (fieldNames.length != 1) {
            throw new ApiException(
                "Expected single-field partition reference"
                    + " but got: "
                    + String.join(".", fieldNames));
          }
          partitionColNames.add(fieldNames[0]);
          break;
        case "cluster_by":
          break;
        default:
          throw new ApiException("Unsupported partition transform: " + t.name());
      }
    }

    List<ColumnInfo> columns = new ArrayList<>();
    StructField[] schemaFields = schema.fields();
    for (int i = 0; i < schemaFields.length; i++) {
      StructField field = schemaFields[i];
      ColumnInfo col = new ColumnInfo();
      col.setName(field.name());
      if (field.getComment().isDefined()) {
        col.setComment(field.getComment().get());
      }
      col.setNullable(field.nullable());
      col.setTypeText(field.dataType().catalogString());
      col.setTypeName(UCProxy.convertDataTypeToTypeName(field.dataType()));
      col.setTypeJson(field.dataType().json());
      col.setPosition(i);
      int partIdx = -1;
      for (int j = 0; j < partitionColNames.size(); j++) {
        if (partitionColNames.get(j).equalsIgnoreCase(field.name())) {
          partIdx = j;
          break;
        }
      }
      if (partIdx >= 0) {
        col.setPartitionIndex(partIdx);
      }
      columns.add(col);
    }

    String comment = properties.get(TableCatalog.PROP_COMMENT);
    if (comment != null) {
      createTable.setComment(comment);
    }
    createTable.setColumns(columns);
    createTable.setDataSourceFormat(UCProxy.convertDatasourceFormat(format));
    Map<String, String> propsToServer = new HashMap<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      if (!UCTableProperties.V2_TABLE_PROPERTIES.contains(e.getKey())) {
        propsToServer.put(e.getKey(), e.getValue());
      }
    }
    createTable.setProperties(propsToServer);
    tablesApi.createTable(createTable);
    return loadTable(catalogName, ident, false, false, false, "");
  }

  @Override
  public Table alterTable(String catalogName, Identifier ident, TableChange... changes)
      throws Exception {
    throw new UnsupportedOperationException(
        "Altering a table is not supported via the legacy UC API");
  }

  @Override
  public boolean dropTable(String catalogName, Identifier ident) throws Exception {
    Object ret = tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(catalogName, ident));
    return ret instanceof Number && ((Number) ret).intValue() == 200;
  }

  @Override
  public void renameTable(String catalogName, Identifier from, Identifier to) throws Exception {
    throw new UnsupportedOperationException("Renaming a table is not supported yet");
  }

  // -- Staging & credential vending --

  @Override
  public Map<String, String> stageManagedTableAndGetProps(
      String catalogName,
      Identifier ident,
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception {
    CreateStagingTable createStagingTable =
        new CreateStagingTable()
            .catalogName(catalogName)
            .schemaName(ident.namespace()[0])
            .name(ident.name());
    StagingTableInfo stagingInfo = tablesApi.createStagingTable(createStagingTable);
    String stagingLocation = stagingInfo.getStagingLocation();
    String stagingTableId = stagingInfo.getId();

    HashMap<String, String> newProps = new HashMap<>(properties);
    newProps.put(TableCatalog.PROP_LOCATION, stagingLocation);
    newProps.put(UCTableProperties.UC_TABLE_ID_KEY, stagingTableId);
    newProps.put(UCTableProperties.UC_CATALOG_NAME_KEY, catalogName);
    newProps.put(UCTableProperties.UC_SCHEMA_NAME_KEY, ident.namespace()[0]);
    newProps.put(UCTableProperties.UC_TABLE_NAME_KEY, ident.name());
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");

    TemporaryCredentials tempCreds =
        temporaryCredentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(stagingTableId)
                .operation(TableOperation.READ_WRITE));
    Map<String, String> credProps =
        CredPropsUtil.createTableCredProps(
            renewCredEnabled,
            credScopedFsEnabled,
            UCSingleCatalog.sessionHadoopFsImplProps(),
            CatalogUtils.stringToURI(stagingLocation).getScheme(),
            ucUri,
            tokenProvider,
            stagingTableId,
            TableOperation.READ_WRITE,
            tempCreds);
    UCSingleCatalog.setCredentialProps(newProps, credProps);
    return newProps;
  }

  @Override
  public Map<String, String> prepareReplaceProps(
      String catalogName,
      Identifier ident,
      String existingTableLocation,
      String existingTableId,
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception {
    HashMap<String, String> newProps = new HashMap<>(properties);
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");

    TemporaryCredentials tempCreds =
        temporaryCredentialsApi.generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(existingTableId)
                .operation(TableOperation.READ_WRITE));
    String scheme = new Path(existingTableLocation).toUri().getScheme();
    Map<String, String> credProps =
        CredPropsUtil.createTableCredProps(
            renewCredEnabled,
            credScopedFsEnabled,
            UCSingleCatalog.sessionHadoopFsImplProps(),
            scheme,
            ucUri,
            tokenProvider,
            existingTableId,
            TableOperation.READ_WRITE,
            tempCreds);
    UCSingleCatalog.setCredentialProps(newProps, credProps);
    return newProps;
  }

  @Override
  public Map<String, String> prepareExternalTableProps(
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception {
    String location = properties.get(TableCatalog.PROP_LOCATION);
    assert location != null;
    TemporaryCredentials cred =
        temporaryCredentialsApi.generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential()
                .url(location)
                .operation(PathOperation.PATH_CREATE_TABLE));
    HashMap<String, String> newProps = new HashMap<>(properties);
    Map<String, String> credProps =
        CredPropsUtil.createPathCredProps(
            renewCredEnabled,
            credScopedFsEnabled,
            UCSingleCatalog.sessionHadoopFsImplProps(),
            CatalogUtils.stringToURI(location).getScheme(),
            ucUri,
            tokenProvider,
            location,
            PathOperation.PATH_CREATE_TABLE,
            cred);
    UCSingleCatalog.setCredentialProps(newProps, credProps);
    return newProps;
  }

  @Override
  public Optional<ExistingTableInfo> resolveExistingTable(
      String catalogName, Identifier ident, boolean allowMissing) throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    String fullName = UCSingleCatalog.fullTableNameForApi(catalogName, ident);
    try {
      TableInfo tableInfo = tablesApi.getTable(fullName, false, false);
      Map<String, String> props = tableInfo.getProperties();
      boolean isCatalogManaged =
          tableInfo.getTableType() == TableType.MANAGED
              && tableInfo.getDataSourceFormat() == DataSourceFormat.DELTA
              && props != null
              && UCTableProperties.DELTA_CATALOG_MANAGED_VALUE.equals(
                  props.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW));
      return Optional.of(
          new ExistingTableInfo(
              tableInfo.getStorageLocation(),
              tableInfo.getTableId(),
              isCatalogManaged,
              tableInfo.getDataSourceFormat().getValue().toLowerCase(Locale.ROOT),
              props));
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        if (allowMissing) {
          return Optional.empty();
        }
        throw new NoSuchTableException(ident);
      }
      throw e;
    }
  }
}
