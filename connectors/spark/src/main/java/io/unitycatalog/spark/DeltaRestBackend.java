package io.unitycatalog.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.AssertEtag;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.CredentialOperation;
import io.unitycatalog.client.delta.model.CredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.ListTablesResponse;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.SetPropertiesUpdate;
import io.unitycatalog.client.delta.model.StagingTableResponse;
import io.unitycatalog.client.delta.model.StagingTableResponseRequiredProtocol;
import io.unitycatalog.client.delta.model.StorageCredential;
import io.unitycatalog.client.delta.model.StorageCredentialConfig;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableIdentifierWithDataSourceFormat;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.model.UpdateTableRequest;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TableOperation;
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
import java.util.Set;
import java.util.UUID;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;

/**
 * {@link CatalogBackend} implementation using the new Delta REST Catalog API ({@link TablesApi},
 * {@link TemporaryCredentialsApi}) at /api/2.1/unity-catalog/delta/v1.
 */
public class DeltaRestBackend implements CatalogBackend {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaRestBackend.class);

  private final TablesApi deltaTablesApi;
  private final TemporaryCredentialsApi deltaCredentialsApi;
  private final TokenProvider tokenProvider;
  private final ObjectMapper objectMapper;

  public DeltaRestBackend(
      TablesApi deltaTablesApi,
      TemporaryCredentialsApi deltaCredentialsApi,
      TokenProvider tokenProvider,
      ObjectMapper objectMapper) {
    this.deltaTablesApi = deltaTablesApi;
    this.deltaCredentialsApi = deltaCredentialsApi;
    this.tokenProvider = tokenProvider;
    this.objectMapper = objectMapper;
  }

  @Override
  public String apiPathLabel() {
    return "delta-rest";
  }

  // -- Table CRUD --

  @Override
  public Identifier[] listTables(String catalogName, String schemaName) throws Exception {
    String[] namespace = new String[] {schemaName};
    List<Identifier> tables = new ArrayList<>();
    String pageToken = null;
    do {
      ListTablesResponse response =
          deltaTablesApi.listTables(catalogName, schemaName, null, pageToken);
      List<TableIdentifierWithDataSourceFormat> ids = response.getIdentifiers();
      if (ids != null) {
        for (TableIdentifierWithDataSourceFormat t : ids) {
          tables.add(Identifier.of(namespace, t.getName()));
        }
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
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    String schemaName = ident.namespace()[0];
    String tableName = ident.name();

    LoadTableResponse response;
    try {
      response = deltaTablesApi.loadTable(catalogName, schemaName, tableName);
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw e;
    }

    TableMetadata metadata = response.getMetadata();
    TableIdentifier identifier =
        new TableIdentifier(tableName, new Some<>(schemaName), new Some<>(catalogName));
    org.apache.spark.sql.types.StructType sparkSchema =
        deltaStructTypeToSparkSchema(metadata.getColumns());
    java.net.URI locationUri = CatalogUtils.stringToURI(metadata.getLocation());
    String tableId = metadata.getTableUuid().toString();

    // Vend credentials via the delta REST API
    CredentialOperation credOp = CredentialOperation.READ_WRITE;
    StorageCredential storageCred;
    try {
      CredentialsResponse credResp =
          deltaCredentialsApi.getTableCredentials(credOp, catalogName, schemaName, tableName);
      storageCred = selectBestCredential(credResp.getStorageCredentials(), metadata.getLocation());
    } catch (ApiException e) {
      LOG.warn("READ_WRITE credential failed for {}: {}", identifier, e.getMessage());
      try {
        credOp = CredentialOperation.READ;
        CredentialsResponse credResp =
            deltaCredentialsApi.getTableCredentials(credOp, catalogName, schemaName, tableName);
        storageCred =
            selectBestCredential(credResp.getStorageCredentials(), metadata.getLocation());
      } catch (ApiException e2) {
        LOG.warn("READ credential failed for {}: {}", identifier, e2.getMessage());
        if (serverSidePlanningEnabled) {
          storageCred = null;
        } else {
          throw e2;
        }
      }
    }

    if (serverSidePlanningEnabled && storageCred == null) {
      UCSingleCatalog.enableServerSidePlanningConfig(identifier);
    }

    TableOperation tableOp =
        credOp == CredentialOperation.READ_WRITE ? TableOperation.READ_WRITE : TableOperation.READ;

    Map<String, String> extraSerdeProps;
    if (storageCred == null) {
      extraSerdeProps = Collections.emptyMap();
    } else {
      TemporaryCredentials tempCreds = storageCredentialToTempCreds(storageCred);
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
              tempCreds,
              true,
              catalogName,
              schemaName,
              tableName);
    }

    Map<String, String> allProps = new HashMap<>();
    if (metadata.getProperties() != null) {
      allProps.putAll(metadata.getProperties());
    }
    allProps.putAll(extraSerdeProps);

    List<String> partCols = metadata.getPartitionColumns();
    if (partCols == null) {
      partCols = Collections.emptyList();
    }

    String dsFormat = metadata.getDataSourceFormat().getValue().toLowerCase(Locale.ROOT);
    Long createdTime = metadata.getCreatedTime();

    CatalogTable sparkTable =
        CatalogTableBuilder.build(
            identifier,
            metadata.getTableType() == io.unitycatalog.client.delta.model.TableType.MANAGED,
            locationUri,
            allProps,
            sparkSchema,
            dsFormat,
            createdTime != null ? createdTime : 0L,
            partCols);

    return CatalogTableBuilder.toV1Table(sparkTable);
  }

  @Override
  public Table createTable(
      String catalogName,
      Identifier ident,
      org.apache.spark.sql.types.StructType schema,
      Transform[] partitions,
      Map<String, String> properties)
      throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    UCSingleCatalog.requireProviderSpecified("CREATE TABLE", properties);

    String schemaName = ident.namespace()[0];
    String format = properties.get("provider");

    CreateTableRequest request = new CreateTableRequest();
    request.setName(ident.name());
    request.setDataSourceFormat(
        io.unitycatalog.client.delta.model.DataSourceFormat.fromValue(format.toUpperCase()));

    boolean hasExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL);
    String storageLocation = properties.get(TableCatalog.PROP_LOCATION);
    assert storageLocation != null;
    boolean isManagedLocation =
        "true".equalsIgnoreCase(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION));
    if (isManagedLocation) {
      assert !hasExternal;
      request.setTableType(io.unitycatalog.client.delta.model.TableType.MANAGED);
    } else {
      request.setTableType(io.unitycatalog.client.delta.model.TableType.EXTERNAL);
    }
    request.setLocation(storageLocation);

    // Convert Spark StructType to delta StructType via JSON
    StructType deltaColumns = sparkSchemaToDeltaStructType(schema);
    request.setColumns(deltaColumns);

    // Extract partition column names
    List<String> partColNames = new ArrayList<>();
    for (Transform t : partitions) {
      switch (t.name()) {
        case "identity":
          String[] fnames = t.references()[0].fieldNames();
          if (fnames.length != 1) {
            throw new ApiException("Expected single-field partition reference");
          }
          partColNames.add(fnames[0]);
          break;
        case "cluster_by":
          break;
        default:
          throw new ApiException("Unsupported partition transform: " + t.name());
      }
    }
    if (!partColNames.isEmpty()) {
      request.setPartitionColumns(partColNames);
    }

    String comment = properties.get(TableCatalog.PROP_COMMENT);
    if (comment != null) {
      request.setComment(comment);
    }

    // Extract Delta protocol. For managed tables, the staging response provides a properly
    // categorized protocol (correct reader/writer feature split). For external tables, we
    // reconstruct from flat delta.* properties.
    request.setProtocol(resolveProtocol(properties));

    Map<String, String> propsToServer = new HashMap<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String key = e.getKey();
      if (!UCTableProperties.V2_TABLE_PROPERTIES.contains(key) && !isDerivedProperty(key)) {
        propsToServer.put(key, e.getValue());
      }
    }
    request.setProperties(propsToServer);

    deltaTablesApi.createTable(catalogName, schemaName, request);
    return loadTable(catalogName, ident, false, false, false, "");
  }

  @Override
  public Table alterTable(String catalogName, Identifier ident, TableChange... changes)
      throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    String schemaName = ident.namespace()[0];
    String tableName = ident.name();

    // Load current table to get etag for optimistic concurrency
    LoadTableResponse current = deltaTablesApi.loadTable(catalogName, schemaName, tableName);
    String etag = current.getMetadata() != null ? current.getMetadata().getEtag() : null;

    // Build updates from TableChange objects
    Map<String, String> propsToSet = new HashMap<>();
    List<String> propsToRemove = new ArrayList<>();
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty sp = (TableChange.SetProperty) change;
        propsToSet.put(sp.property(), sp.value());
      } else if (change instanceof TableChange.RemoveProperty) {
        TableChange.RemoveProperty rp = (TableChange.RemoveProperty) change;
        propsToRemove.add(rp.property());
      } else {
        throw new UnsupportedOperationException(
            "Unsupported table change type: " + change.getClass().getSimpleName());
      }
    }

    UpdateTableRequest request = new UpdateTableRequest();
    if (etag != null) {
      request.addRequirementsItem(new AssertEtag().etag(etag));
    }
    if (!propsToSet.isEmpty()) {
      request.addUpdatesItem(new SetPropertiesUpdate().updates(propsToSet));
    }
    if (!propsToRemove.isEmpty()) {
      request.addUpdatesItem(new RemovePropertiesUpdate().removals(propsToRemove));
    }

    deltaTablesApi.updateTable(catalogName, schemaName, tableName, request);

    // Reload and return the updated table
    return loadTable(catalogName, ident, false, false, false, "");
  }

  @Override
  public boolean dropTable(String catalogName, Identifier ident) throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    try {
      deltaTablesApi.deleteTable(catalogName, ident.namespace()[0], ident.name());
      return true;
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        return false;
      }
      throw e;
    }
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
    String schemaName = ident.namespace()[0];
    CreateStagingTableRequest request = new CreateStagingTableRequest().name(ident.name());
    StagingTableResponse stagingResp =
        deltaTablesApi.createStagingTable(catalogName, schemaName, request);

    String stagingLocation = stagingResp.getLocation();
    String stagingTableId = stagingResp.getTableId().toString();

    HashMap<String, String> newProps = new HashMap<>(properties);
    newProps.put(TableCatalog.PROP_LOCATION, stagingLocation);
    newProps.put(UCTableProperties.UC_TABLE_ID_KEY, stagingTableId);
    newProps.put(UCTableProperties.UC_CATALOG_NAME_KEY, catalogName);
    newProps.put(UCTableProperties.UC_SCHEMA_NAME_KEY, schemaName);
    newProps.put(UCTableProperties.UC_TABLE_NAME_KEY, ident.name());
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true");

    // Save the staging response's required-protocol so createTable() can use the
    // properly categorized reader/writer features instead of reconstructing from flat
    // properties.
    StagingTableResponseRequiredProtocol requiredProtocol = stagingResp.getRequiredProtocol();
    if (requiredProtocol != null) {
      try {
        newProps.put(STAGING_PROTOCOL_KEY, objectMapper.writeValueAsString(requiredProtocol));
      } catch (Exception e) {
        LOG.warn("Failed to serialize staging protocol, will reconstruct from properties", e);
      }
    }

    // Merge required and suggested properties
    if (stagingResp.getRequiredProperties() != null) {
      for (Map.Entry<String, String> e : stagingResp.getRequiredProperties().entrySet()) {
        if (e.getValue() != null) {
          newProps.put(e.getKey(), e.getValue());
        }
      }
    }
    if (stagingResp.getSuggestedProperties() != null) {
      for (Map.Entry<String, String> e : stagingResp.getSuggestedProperties().entrySet()) {
        if (e.getValue() != null) {
          newProps.putIfAbsent(e.getKey(), e.getValue());
        }
      }
    }

    // Use inline credentials from the staging response
    StorageCredential storageCred =
        selectBestCredential(stagingResp.getStorageCredentials(), stagingLocation);
    if (storageCred != null) {
      TemporaryCredentials tempCreds = storageCredentialToTempCreds(storageCred);
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
    }
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

    CredentialsResponse credResp =
        deltaCredentialsApi.getStagingTableCredentials(UUID.fromString(existingTableId));
    StorageCredential storageCred =
        selectBestCredential(credResp.getStorageCredentials(), existingTableLocation);
    if (storageCred != null) {
      TemporaryCredentials tempCreds = storageCredentialToTempCreds(storageCred);
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
    }
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
    CredentialsResponse credResp =
        deltaCredentialsApi.getTemporaryPathCredentials(location, CredentialOperation.READ_WRITE);
    StorageCredential storageCred =
        selectBestCredential(credResp.getStorageCredentials(), location);

    HashMap<String, String> newProps = new HashMap<>(properties);
    if (storageCred != null) {
      TemporaryCredentials tempCreds = storageCredentialToTempCreds(storageCred);
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
              tempCreds,
              true);
      UCSingleCatalog.setCredentialProps(newProps, credProps);
    }
    return newProps;
  }

  @Override
  public Optional<ExistingTableInfo> resolveExistingTable(
      String catalogName, Identifier ident, boolean allowMissing) throws Exception {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace());
    try {
      LoadTableResponse response =
          deltaTablesApi.loadTable(catalogName, ident.namespace()[0], ident.name());
      TableMetadata metadata = response.getMetadata();
      Map<String, String> props = metadata.getProperties();
      boolean isCatalogManaged =
          metadata.getTableType() == io.unitycatalog.client.delta.model.TableType.MANAGED
              && metadata.getDataSourceFormat()
                  == io.unitycatalog.client.delta.model.DataSourceFormat.DELTA
              && props != null
              && UCTableProperties.DELTA_CATALOG_MANAGED_VALUE.equals(
                  props.get(UCTableProperties.DELTA_CATALOG_MANAGED_KEY_NEW));
      return Optional.of(
          new ExistingTableInfo(
              metadata.getLocation(),
              metadata.getTableUuid().toString(),
              isCatalogManaged,
              metadata.getDataSourceFormat().getValue().toLowerCase(Locale.ROOT),
              props != null ? props : new HashMap<>()));
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

  // -- Conversion utilities --

  /** Converts the delta REST API StructType to Spark StructType via JSON roundtrip. */
  private org.apache.spark.sql.types.StructType deltaStructTypeToSparkSchema(StructType deltaType) {
    try {
      String json = objectMapper.writeValueAsString(deltaType);
      return (org.apache.spark.sql.types.StructType) DataType.fromJson(json);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert delta schema to Spark schema", e);
    }
  }

  /** Converts a Spark StructType to the delta REST API StructType via JSON roundtrip. */
  private StructType sparkSchemaToDeltaStructType(
      org.apache.spark.sql.types.StructType sparkSchema) {
    try {
      String json = sparkSchema.json();
      return objectMapper.readValue(json, StructType.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert Spark schema to delta schema", e);
    }
  }

  /** Selects the StorageCredential with the longest matching prefix from a list. */
  static StorageCredential selectBestCredential(
      List<StorageCredential> credentials, String location) {
    if (credentials == null || credentials.isEmpty()) {
      return null;
    }
    if (credentials.size() == 1) {
      return credentials.get(0);
    }
    return credentials.stream()
        .filter(c -> location.startsWith(c.getPrefix()))
        .max(Comparator.comparingInt(c -> c.getPrefix().length()))
        .orElse(credentials.get(0));
  }

  /**
   * Converts the new delta REST API StorageCredential to the legacy TemporaryCredentials model that
   * CredPropsUtil expects.
   */
  static TemporaryCredentials storageCredentialToTempCreds(StorageCredential storageCred) {
    StorageCredentialConfig config = storageCred.getConfig();
    TemporaryCredentials tempCreds = new TemporaryCredentials();

    if (storageCred.getExpirationTimeMs() != null) {
      tempCreds.setExpirationTime(storageCred.getExpirationTimeMs());
    }

    if (config.getS3AccessKeyId() != null) {
      AwsCredentials awsCred = new AwsCredentials();
      awsCred.setAccessKeyId(config.getS3AccessKeyId());
      awsCred.setSecretAccessKey(config.getS3SecretAccessKey());
      awsCred.setSessionToken(config.getS3SessionToken());
      tempCreds.setAwsTempCredentials(awsCred);
    } else if (config.getGcsOauthToken() != null) {
      GcpOauthToken gcpToken = new GcpOauthToken();
      gcpToken.setOauthToken(config.getGcsOauthToken());
      tempCreds.setGcpOauthToken(gcpToken);
    } else if (config.getAzureSasToken() != null) {
      AzureUserDelegationSAS azureSas = new AzureUserDelegationSAS();
      azureSas.setSasToken(config.getAzureSasToken());
      tempCreds.setAzureUserDelegationSas(azureSas);
    }

    return tempCreds;
  }

  /**
   * Internal property key used to pass the staging response's structured protocol from
   * stageManagedTableAndGetProps() to createTable(). Removed before sending to the server.
   */
  private static final String STAGING_PROTOCOL_KEY = "io.unitycatalog.internal.stagingProtocol";

  private static final String DELTA_MIN_READER_VERSION = "delta.minReaderVersion";
  private static final String DELTA_MIN_WRITER_VERSION = "delta.minWriterVersion";
  private static final String DELTA_FEATURE_PREFIX = "delta.feature.";

  /**
   * Property keys that the Delta REST API server considers "derived" and rejects in createTable
   * properties. These must be conveyed via structured protocol/metadata fields instead.
   */
  private static final Set<String> DERIVED_PROPERTY_KEYS =
      Set.of(
          "clusteringColumns",
          "delta.lastCommitTimestamp",
          "delta.lastUpdateVersion",
          DELTA_MIN_READER_VERSION,
          DELTA_MIN_WRITER_VERSION);

  /** Returns true if the property key is a derived property that the server rejects. */
  private static boolean isDerivedProperty(String key) {
    return DERIVED_PROPERTY_KEYS.contains(key)
        || key.equals(STAGING_PROTOCOL_KEY)
        || key.startsWith(DELTA_FEATURE_PREFIX)
        || key.startsWith("delta.domainMetadata.")
        || key.startsWith("delta.rowTracking");
  }

  /**
   * Resolves the Delta protocol for a createTable request. For managed tables that went through the
   * staging flow, the properly categorized protocol (with correct reader/writer feature split) is
   * passed via a serialized property. For external tables, we reconstruct from flat delta.*
   * properties — the reader/writer distinction is lost in this case because Spark's TableCatalog
   * API only passes flat Map&lt;String, String&gt; properties.
   */
  private DeltaProtocol resolveProtocol(Map<String, String> properties) {
    // Prefer the staging response's structured protocol (correct reader/writer categorization)
    String stagingProtocolJson = properties.get(STAGING_PROTOCOL_KEY);
    if (stagingProtocolJson != null) {
      try {
        return objectMapper.readValue(stagingProtocolJson, DeltaProtocol.class);
      } catch (Exception e) {
        LOG.warn("Failed to deserialize staging protocol, falling back to extraction", e);
      }
    }
    // Fallback for external tables: reconstruct from flat properties
    return extractProtocolFromProperties(properties);
  }

  /**
   * Reconstructs a DeltaProtocol from flat table properties. This is best-effort: the reader/writer
   * feature distinction is not preserved in flat properties, so all features are placed in
   * writerFeatures only.
   */
  private static DeltaProtocol extractProtocolFromProperties(Map<String, String> properties) {
    DeltaProtocol protocol = new DeltaProtocol();
    String minReader = properties.get(DELTA_MIN_READER_VERSION);
    String minWriter = properties.get(DELTA_MIN_WRITER_VERSION);
    protocol.setMinReaderVersion(minReader != null ? Integer.parseInt(minReader) : 1);
    protocol.setMinWriterVersion(minWriter != null ? Integer.parseInt(minWriter) : 2);

    List<String> features = new ArrayList<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      if (e.getKey().startsWith(DELTA_FEATURE_PREFIX) && "supported".equals(e.getValue())) {
        features.add(e.getKey().substring(DELTA_FEATURE_PREFIX.length()));
      }
    }
    if (!features.isEmpty()) {
      protocol.setWriterFeatures(features);
      protocol.setReaderFeatures(null);
    }
    return protocol;
  }
}
