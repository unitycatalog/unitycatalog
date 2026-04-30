package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.CreateTableRequest;
import io.unitycatalog.server.delta.model.DataSourceFormat;
import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.delta.model.PrimitiveType;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.model.StructFieldMetadata;
import io.unitycatalog.server.delta.model.StructType;
import io.unitycatalog.server.delta.model.TableType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    CreateTable created = DeltaCreateTableMapper.toCreateTable("cat", "sch", baseManagedRequest());

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
    CreateTableRequest req =
        baseExternalRequest()
            .protocol(
                new DeltaProtocol()
                    .minReaderVersion(3)
                    .minWriterVersion(7)
                    .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                    .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
            .properties(Map.of());

    CreateTable created = DeltaCreateTableMapper.toCreateTable("cat", "sch", req);
    assertThat(created.getTableType()).isEqualTo(io.unitycatalog.server.model.TableType.EXTERNAL);
  }

  @Test
  public void unsupportedDataSourceFormatRejected() {
    // Use EXTERNAL so the contract gate doesn't fire first; the format gate is what's under test.
    CreateTableRequest req = baseExternalRequest().dataSourceFormat(DataSourceFormat.ICEBERG);
    assertThatThrownBy(() -> DeltaCreateTableMapper.toCreateTable("cat", "sch", req))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("Unsupported data-source-format");
  }

  // --- fixtures ---

  /** A canonical fully-compliant MANAGED createTable request. */
  private static CreateTableRequest baseManagedRequest() {
    return new CreateTableRequest()
        .name("tbl")
        .location("s3://b/p")
        .tableType(TableType.MANAGED)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(simpleColumns())
        .protocol(managedProtocol())
        .properties(fullManagedProperties("uuid-x"));
  }

  /** A minimal EXTERNAL createTable request (UC mirrors whatever the client sends). */
  private static CreateTableRequest baseExternalRequest() {
    return new CreateTableRequest()
        .name("tbl")
        .location("s3://b/p")
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .columns(simpleColumns())
        .protocol(
            new DeltaProtocol()
                .minReaderVersion(3)
                .minWriterVersion(7)
                .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
        .properties(Map.of());
  }

  private static StructType simpleColumns() {
    return new StructType()
        .type("struct")
        .fields(
            List.of(
                new StructField()
                    .name("id")
                    .type(new PrimitiveType().type("long"))
                    .nullable(false)
                    .metadata(new StructFieldMetadata())));
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
    Map<String, String> props = new HashMap<>();
    props.put(TableProperties.CHECKPOINT_POLICY, "v2");
    props.put(TableProperties.ENABLE_DELETION_VECTORS, "true");
    props.put(TableProperties.ENABLE_IN_COMMIT_TIMESTAMPS, "true");
    props.put(TableProperties.UC_TABLE_ID, tableId);
    // Engine-generated values: any non-null placeholder satisfies the contract.
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, "0");
    props.put(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, "1700000000000");
    return props;
  }

  private static String featureKey(String feature) {
    return TableProperties.FEATURE_PREFIX + feature;
  }
}
