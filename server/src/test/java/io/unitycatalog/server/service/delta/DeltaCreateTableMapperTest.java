package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Mapper-boundary unit tests for {@link DeltaCreateTableMapper}. The validation rules and the
 * property projections themselves are owned by {@link UcManagedDeltaContract} and {@link
 * DeltaPropertyMapper}, and are tested in their respective test files. This file only pins the
 * orchestration the mapper itself adds: end-to-end request → CreateTable shape, the MANAGED-only
 * contract gate, and the DELTA-only format gate.
 */
public class DeltaCreateTableMapperTest {

  @Test
  public void happyPathManagedBuildsCreateTable() {
    DeltaCreateTableMapper.Result result =
        DeltaCreateTableMapper.toCreateTable(
            "cat", "sch", baseManagedRequest(), new ServerProperties());
    assertThat(result.baseTableId()).isEmpty();
    CreateTable created = result.createTable();

    assertThat(created.getName()).isEqualTo("tbl");
    assertThat(created.getCatalogName()).isEqualTo("cat");
    assertThat(created.getSchemaName()).isEqualTo("sch");
    assertThat(created.getTableType()).isEqualTo(io.unitycatalog.server.model.TableType.MANAGED);
    assertThat(created.getDataSourceFormat())
        .isEqualTo(io.unitycatalog.server.model.DataSourceFormat.DELTA);
    assertThat(created.getStorageLocation()).isEqualTo("s3://b/p");
    assertThat(created.getColumns()).hasSize(1);
    assertThat(created.getColumns().get(0).getName()).isEqualTo("id");
    // Confirms the DeltaPropertyMapper merge ran end-to-end: a derived feature property and a
    // client-supplied property both land in the merged map.
    assertThat(created.getProperties())
        .containsEntry(featureKey(TableFeature.CATALOG_MANAGED.specName()), "supported")
        .containsEntry(TableProperties.UC_TABLE_ID, "uuid-x");
  }

  @Test
  public void externalTableSkipsUcManagedContract() {
    // Pins the MANAGED-only gate: an EXTERNAL request that would fail UcManagedDeltaContract
    // (missing catalogManaged, empty properties) succeeds because the gate routes around it.
    DeltaCreateTableRequest req =
        baseExternalRequest()
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                    .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
            .properties(Map.of());

    CreateTable created =
        DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties())
            .createTable();
    assertThat(created.getTableType()).isEqualTo(io.unitycatalog.server.model.TableType.EXTERNAL);
  }

  // ---------- shallow clone ----------

  @Test
  public void managedShallowCloneMapsToManagedWithBaseTableId() {
    DeltaCreateTableMapper.Result result =
        DeltaCreateTableMapper.toCreateTable(
            "cat", "sch", baseManagedShallowCloneRequest(), new ServerProperties());

    // Stored as a plain MANAGED table; the clone relationship rides separately as baseTableId.
    assertThat(result.createTable().getTableType())
        .isEqualTo(io.unitycatalog.server.model.TableType.MANAGED);
    assertThat(result.baseTableId()).contains(BASE_TABLE_ID);
  }

  @Test
  public void managedShallowCloneEnforcesUcManagedContract() {
    // The MANAGED-only contract gate applies to clones too: a clone request whose protocol lacks
    // catalogManaged must fail exactly like a plain MANAGED request would.
    DeltaCreateTableRequest req =
        baseManagedShallowCloneRequest()
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                    .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())));

    assertThatThrownBy(
            () -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.CATALOG_MANAGED.specName());
  }

  @Test
  public void managedShallowCloneRequiresBaseTableId() {
    DeltaCreateTableRequest req = baseManagedShallowCloneRequest().baseTableId(null);

    assertThatThrownBy(
            () -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("base-table-id is required");
  }

  @Test
  public void baseTableIdRejectedForNonCloneTypes() {
    for (DeltaCreateTableRequest req :
        List.of(
            baseManagedRequest().baseTableId(BASE_TABLE_ID),
            baseExternalRequest().baseTableId(BASE_TABLE_ID))) {
      assertThatThrownBy(
              () -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties()))
          .isInstanceOf(BaseException.class)
          .hasMessageContaining("base-table-id must not be set");
    }
  }

  @Test
  public void externalShallowCloneIsUnimplemented() {
    DeltaCreateTableRequest req =
        baseExternalRequest()
            .tableType(DeltaTableType.EXTERNAL_SHALLOW_CLONE)
            .baseTableId(BASE_TABLE_ID);

    assertThatThrownBy(
            () -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e -> assertThat(((BaseException) e).getErrorCode()).isEqualTo(ErrorCode.UNIMPLEMENTED))
        .hasMessageContaining("EXTERNAL_SHALLOW_CLONE");
  }

  // ---------- allowMissingDvForUniformV2 flag ----------

  @Test
  public void allowMissingDvFlagAcceptsUniformV2RequestWithoutDv() {
    // A UniForm Iceberg request without DV feature/property is rejected by default but accepted
    // when server.managed-table.uniform-iceberg-v2.allow-missing-dv=true.
    assertThatThrownBy(
            () ->
                DeltaCreateTableMapper.toCreateTable(
                    "cat", "sch", uniformV2RequestWithoutDv(), new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());

    assertThatCode(
            () ->
                DeltaCreateTableMapper.toCreateTable(
                    "cat",
                    "sch",
                    uniformV2RequestWithoutDv(),
                    TestUtils.serverPropertiesWithAllowMissingDv()))
        .doesNotThrowAnyException();
  }

  @Test
  public void allowMissingDvFlagDoesNotSkipDvWithoutIcebergCompatV2Property() {
    // UniForm Iceberg enabled but delta.enableIcebergCompatV2 not set -- DV is still required.
    DeltaCreateTableRequest req = uniformV2RequestWithoutDv();
    req.getProperties().remove(TableProperties.ENABLE_ICEBERG_COMPAT_V2);
    assertThatThrownBy(
            () ->
                DeltaCreateTableMapper.toCreateTable(
                    "cat", "sch", req, TestUtils.serverPropertiesWithAllowMissingDv()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  @Test
  public void allowMissingDvFlagOffRejectsMissingDvWithoutIcebergCompatV2Property() {
    // flag=false, no delta.enableIcebergCompatV2 -- DV is required regardless.
    DeltaCreateTableRequest req = uniformV2RequestWithoutDv();
    req.getProperties().remove(TableProperties.ENABLE_ICEBERG_COMPAT_V2);
    assertThatThrownBy(
            () -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req, new ServerProperties()))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(TableFeature.DELETION_VECTORS.specName());
  }

  // --- fixtures ---

  private static final UUID BASE_TABLE_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

  /**
   * A canonical fully-compliant MANAGED_SHALLOW_CLONE createTable request: identical to {@link
   * #baseManagedRequest()} except for the table type and the base-table-id that comes with it.
   */
  private static DeltaCreateTableRequest baseManagedShallowCloneRequest() {
    return baseManagedRequest()
        .tableType(DeltaTableType.MANAGED_SHALLOW_CLONE)
        .baseTableId(BASE_TABLE_ID);
  }

  /** A canonical fully-compliant MANAGED createTable request. */
  private static DeltaCreateTableRequest baseManagedRequest() {
    return new DeltaCreateTableRequest()
        .name("tbl")
        .location("s3://b/p")
        .tableType(DeltaTableType.MANAGED)
        .columns(simpleColumns())
        .protocol(managedProtocol())
        .properties(fullManagedProperties("uuid-x"))
        .lastCommitTimestampMs(1700000000000L);
  }

  /** A minimal EXTERNAL createTable request (UC mirrors whatever the client sends). */
  private static DeltaCreateTableRequest baseExternalRequest() {
    return new DeltaCreateTableRequest()
        .name("tbl")
        .location("s3://b/p")
        .tableType(DeltaTableType.EXTERNAL)
        .columns(simpleColumns())
        .protocol(
            new DeltaProtocol()
                .minReaderVersion(3)
                .minWriterVersion(7)
                .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
        .properties(Map.of())
        .lastCommitTimestampMs(1700000000000L);
  }

  private static DeltaStructType simpleColumns() {
    return new DeltaStructType()
        .type("struct")
        .fields(
            List.of(
                new DeltaStructField()
                    .name("id")
                    .type(new DeltaPrimitiveType().type("long"))
                    .nullable(false)
                    .metadata(new DeltaStructFieldMetadata())));
  }

  private static DeltaProtocol managedProtocol() {
    return new DeltaProtocol()
        .minReaderVersion(3)
        .minWriterVersion(7)
        .readerFeatures(
            List.of(
                TableFeature.CATALOG_MANAGED.specName(),
                TableFeature.DELETION_VECTORS.specName(),
                TableFeature.V2_CHECKPOINT.specName(),
                TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
        .writerFeatures(
            List.of(
                TableFeature.CATALOG_MANAGED.specName(),
                TableFeature.DELETION_VECTORS.specName(),
                TableFeature.V2_CHECKPOINT.specName(),
                TableFeature.VACUUM_PROTOCOL_CHECK.specName(),
                TableFeature.IN_COMMIT_TIMESTAMP.specName()));
  }

  private static Map<String, String> fullManagedProperties(String tableId) {
    Map<String, String> props = new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.put(TableProperties.UC_TABLE_ID, tableId);
    return props;
  }

  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }

  /**
   * A MANAGED createTable request with delta.enableIcebergCompatV2=true but no uniform block and no
   * delta.universalFormat.enabledFormats. Omits DV feature and property. Used to test that the
   * allowMissingDvForUniformV2 flag skips DV enforcement based solely on IcebergCompatV2.
   */
  private static DeltaCreateTableRequest uniformV2RequestWithoutDv() {
    Map<String, String> props = new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.remove(TableProperties.ENABLE_DELETION_VECTORS);
    props.put(TableProperties.UC_TABLE_ID, "uuid-uniform-v2");
    props.put(TableProperties.ENABLE_ICEBERG_COMPAT_V2, "true");

    DeltaProtocol protocol =
        new DeltaProtocol()
            .minReaderVersion(3)
            .minWriterVersion(7)
            .readerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName()))
            .writerFeatures(
                List.of(
                    TableFeature.CATALOG_MANAGED.specName(),
                    TableFeature.V2_CHECKPOINT.specName(),
                    TableFeature.VACUUM_PROTOCOL_CHECK.specName(),
                    TableFeature.IN_COMMIT_TIMESTAMP.specName()));

    return new DeltaCreateTableRequest()
        .name("tbl")
        .location("s3://b/p")
        .tableType(DeltaTableType.MANAGED)
        .columns(simpleColumns())
        .protocol(protocol)
        .properties(props)
        .lastCommitTimestampMs(1700000000000L);
  }
}
