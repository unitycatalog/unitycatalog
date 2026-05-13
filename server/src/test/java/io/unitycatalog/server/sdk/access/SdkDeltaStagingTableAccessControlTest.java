package io.unitycatalog.server.sdk.access;

import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.CreateStagingTableRequest;
import io.unitycatalog.client.delta.model.CreateTableRequest;
import io.unitycatalog.client.delta.model.DataSourceFormat;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.PrimitiveType;
import io.unitycatalog.client.delta.model.StagingTableResponse;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.StructType;
import io.unitycatalog.client.delta.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.UcManagedDeltaContract;
import io.unitycatalog.server.utils.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Runs the staging-table access-control suite against the Delta REST createStagingTable /
 * createTable. The persist-layer cross-principal check at {@code
 * StagingTableRepository.commitStagingTable} is shared with UC REST, so finalize must produce the
 * same allow/deny outcomes as {@link SdkUCStagingTableAccessControlTest}.
 */
public class SdkDeltaStagingTableAccessControlTest extends SdkStagingTableAccessControlTest {

  /**
   * Engine-generated property placeholder timestamps; the contract validates presence + non-null.
   */
  private static final String ENGINE_GENERATED_PLACEHOLDER = "1700000000000";

  /** Single-column schema mirroring {@link SdkUCStagingTableAccessControlTest#COLUMNS}. */
  private static final StructType SCHEMA =
      new StructType()
          .type("struct")
          .fields(
              List.of(
                  new StructField()
                      .name("test_column")
                      .type(new PrimitiveType().type("integer"))
                      .nullable(true)
                      .metadata(Map.of())));

  @Override
  protected StagingHandle createStaging(
      ServerConfig config, String catalog, String schema, String name) throws Exception {
    StagingTableResponse resp =
        deltaTablesApi(config)
            .createStagingTable(catalog, schema, new CreateStagingTableRequest().name(name));
    return new StagingHandle(resp.getTableId().toString(), resp.getLocation());
  }

  @Override
  protected FinalizedTable finalizeManagedTable(
      ServerConfig config, StagingHandle staging, String name) throws Exception {
    LoadTableResponse resp =
        deltaTablesApi(config)
            .createTable(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, buildCreateRequest(staging, name));
    return new FinalizedTable(
        resp.getMetadata().getTableUuid().toString(), resp.getMetadata().getLocation());
  }

  @Override
  protected void fetchTempCreds(ServerConfig config, String tableId) throws Exception {
    new TemporaryCredentialsApi(TestUtils.createApiClient(config))
        .getStagingTableCredentials(UUID.fromString(tableId));
  }

  private static TablesApi deltaTablesApi(ServerConfig config) {
    return new TablesApi(TestUtils.createApiClient(config));
  }

  /**
   * Build a MANAGED Delta REST createTable request that satisfies the full UC catalog-managed
   * contract enforced by {@code DeltaCreateTableMapper} -- protocol versions + required features +
   * fixed properties + engine-generated property placeholders + UC_TABLE_ID. Constants come from
   * {@link UcManagedDeltaContract} so this fixture stays in sync if the contract evolves.
   */
  private static CreateTableRequest buildCreateRequest(StagingHandle staging, String name) {
    Map<String, String> properties =
        new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    properties.put(TableProperties.UC_TABLE_ID, staging.id());
    UcManagedDeltaContract.ENGINE_GENERATED_PROPERTY_KEYS.forEach(
        key -> properties.put(key, ENGINE_GENERATED_PLACEHOLDER));
    return new CreateTableRequest()
        .name(name)
        .location(staging.location())
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .protocol(
            new DeltaProtocol()
                .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                .writerFeatures(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES))
        .columns(SCHEMA)
        .properties(properties);
  }
}
