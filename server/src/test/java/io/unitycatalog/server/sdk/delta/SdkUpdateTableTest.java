package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.client.delta.model.DeltaArrayType;
import io.unitycatalog.client.delta.model.DeltaAssertEtag;
import io.unitycatalog.client.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.client.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.DeltaCreateTableRequest;
import io.unitycatalog.client.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaMapType;
import io.unitycatalog.client.delta.model.DeltaPrimitiveType;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DeltaRemoveDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.DeltaRowTrackingDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaSetDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.DeltaSetLatestBackfilledVersionUpdate;
import io.unitycatalog.client.delta.model.DeltaSetPartitionColumnsUpdate;
import io.unitycatalog.client.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.client.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.client.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.client.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaStructField;
import io.unitycatalog.client.delta.model.DeltaStructFieldMetadata;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.delta.model.DeltaTableRequirement;
import io.unitycatalog.client.delta.model.DeltaTableType;
import io.unitycatalog.client.delta.model.DeltaTableUpdate;
import io.unitycatalog.client.delta.model.DeltaUniformMetadata;
import io.unitycatalog.client.delta.model.DeltaUniformMetadataIceberg;
import io.unitycatalog.client.delta.model.DeltaUpdateSnapshotVersionUpdate;
import io.unitycatalog.client.delta.model.DeltaUpdateTableRequest;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.service.delta.UcManagedDeltaContract;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Delta {@code POST /v1/.../tables/{table}} update endpoint. Consolidated
 * into a single test so the server start + catalog/schema creation runs once; each section uses a
 * distinct table name so they don't collide. All tables are created through the Delta API
 * (staging-finalize for MANAGED, direct create for EXTERNAL) -- the UC REST surface is never used.
 */
public class SdkUpdateTableTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    // Needed by the non-Delta guard test to plant a parquet EXTERNAL row.
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testUpdateTableEndpoints() throws Exception {
    // -------- umbrella: bundle every action the API supports on a managed table into one RPC.
    // Seeded with arbitrary client properties so set-properties has something to keep, update, and
    // remove. Features land via set-protocol -- not as pre-seeded properties -- because that's the
    // spec-canonical way the new Delta API conveys them.
    {
      Handle h =
          createDeltaManaged(
              "tbl_umbrella", Map.of("keep", "v1", "update_me", "v_old", "drop_me", "v3"));
      Long t0 =
          deltaTablesApi
              .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name())
              .getMetadata()
              .getUpdatedTime();

      // Must keep the managed-contract required features; adding CLUSTERING proves features are
      // replaced (not merged) by what the request carries.
      List<String> newWriterFeatures =
          new ArrayList<>(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES);
      newWriterFeatures.add(TableFeature.ROW_TRACKING.specName());
      newWriterFeatures.add(TableFeature.CLUSTERING.specName());
      DeltaProtocol newProtocol =
          new DeltaProtocol()
              .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
              .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
              .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
              .writerFeatures(newWriterFeatures);
      DeltaDomainMetadataUpdates newDM =
          new DeltaDomainMetadataUpdates()
              .deltaClustering(
                  new DeltaClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))));

      // Phase 1: every action except remove-domain-metadata. Splitting remove-DM into its own RPC
      // pins that the rowTracking entry only disappears as a result of remove-DM, not as a side
      // effect of phase 1's set-DM (which is intent-based per spec, not a full replacement).
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaSetPropertiesUpdate().updates(Map.of("update_me", "v_new", "added", "v4")),
              new DeltaRemovePropertiesUpdate().removals(List.of("drop_me", "missing")),
              new DeltaSetProtocolUpdate().protocol(newProtocol),
              new DeltaSetDomainMetadataUpdate().updates(newDM),
              new DeltaSetTableCommentUpdate().comment("umbrella comment"));

      Map<String, String> props1 = r1.getMetadata().getProperties();
      assertThat(props1)
          .containsEntry("keep", "v1")
          .containsEntry("update_me", "v_new")
          .containsEntry("added", "v4")
          .doesNotContainKeys("drop_me", "missing");
      // set-protocol fully replaces delta.feature.* with entries derived from the new protocol.
      assertThat(featurePropertiesIn(props1)).isEqualTo(featurePropertiesOf(newProtocol));
      // set-DM is additive: clustering JSON-encoded; rowTracking from create time still present.
      assertThat(props1)
          .containsEntry(TableProperties.CLUSTERING_COLUMNS, "[[\"id\"]]")
          .containsKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK);
      // A metadata change advances updated-time, and with it the etag.
      assertThat(r1.getMetadata().getUpdatedTime()).isGreaterThan(t0);
      assertThat(r1.getMetadata().getEtag()).isNotEqualTo(h.etag());

      // Phase 2: remove-domain-metadata alone -- now the rowTracking entry must disappear.
      DeltaLoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new DeltaRemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking")));
      assertThat(r2.getMetadata().getProperties())
          .doesNotContainKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK)
          .containsKey(TableProperties.CLUSTERING_COLUMNS);

      // Phase 3: a set-domain-metadata touching clustering is a metadata change and rolls the
      // etag, even when the exempt rowTracking high-water mark rides in the same action --
      // contrast with the HWM-only case in the tbl_commit_meta_stamp section.
      DeltaLoadTableResponse r3 =
          updateTable(
              h.withEtag(r2.getMetadata().getEtag()),
              new DeltaSetDomainMetadataUpdate()
                  .updates(
                      new DeltaDomainMetadataUpdates()
                          .deltaClustering(
                              new DeltaClusteringDomainMetadata()
                                  .clusteringColumns(List.of(List.of("name"))))
                          .deltaRowTracking(
                              new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(123L))));
      assertThat(r3.getMetadata().getProperties())
          .containsEntry(TableProperties.CLUSTERING_COLUMNS, "[[\"name\"]]")
          .containsEntry(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, "123");
      assertThat(r3.getMetadata().getEtag()).isNotEqualTo(r2.getMetadata().getEtag());
    }

    // -------- set-protocol-drops-feature + sibling remove-DM in same RPC succeeds (post-apply
    // validation). Follow-up set-DM that re-introduces the dropped domain then fails.
    {
      Handle h = createDeltaManaged("tbl_setproto_drops_dm", Map.of());
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetProtocolUpdate()
                  .protocol(
                      new DeltaProtocol()
                          .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                          .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                          .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                          .writerFeatures(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES)),
              new DeltaRemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking")));
      assertThat(r.getMetadata().getProperties())
          .doesNotContainKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK)
          .doesNotContainKey(TableProperties.FEATURE_PREFIX + "rowTracking");

      Handle h2 = h.withEtag(r.getMetadata().getEtag());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h2,
                  new DeltaSetDomainMetadataUpdate()
                      .updates(
                          new DeltaDomainMetadataUpdates()
                              .deltaRowTracking(
                                  new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(1L)))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "rowTracking");
    }

    // -------- update-metadata-snapshot-version --------
    {
      Handle external = createDeltaExternal("tbl_snapshot_external");

      // Missing required field on EXTERNAL -- run before the happy path so `external`'s etag is
      // still current (rejections don't mutate state, the happy path does). One case per missing
      // field so the per-field error message is exercised separately.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  external, new DeltaUpdateSnapshotVersionUpdate().lastCommitTimestampMs(1L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "requires last-commit-version");
      TestUtils.assertDeltaApiException(
          () -> updateTable(external, new DeltaUpdateSnapshotVersionUpdate().lastCommitVersion(1L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "requires last-commit-timestamp-ms");

      // Happy path on EXTERNAL.
      DeltaLoadTableResponse r =
          updateTable(
              external,
              new DeltaUpdateSnapshotVersionUpdate()
                  .lastCommitVersion(42L)
                  .lastCommitTimestampMs(1700000000001L));
      assertThat(r.getMetadata().getLastCommitVersion()).isEqualTo(42L);
      assertThat(r.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000000001L);

      // MANAGED rejection.
      Handle managed = createDeltaManaged("tbl_snapshot_managed", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  managed,
                  new DeltaUpdateSnapshotVersionUpdate()
                      .lastCommitVersion(1L)
                      .lastCommitTimestampMs(1700000000000L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "EXTERNAL");
    }

    // -------- set-columns + set-partition-columns --------
    // physicalNames equal the new logical names (first-enable without rename).
    {
      Handle h = createDeltaExternal("tbl_setcols");
      DeltaStructFieldMetadata newIdMeta = new DeltaStructFieldMetadata();
      newIdMeta.put("delta.columnMapping.id", 1);
      newIdMeta.put("delta.columnMapping.physicalName", "new_id");
      DeltaStructFieldMetadata flagMeta = new DeltaStructFieldMetadata();
      flagMeta.put("delta.columnMapping.id", 2);
      flagMeta.put("delta.columnMapping.physicalName", "flag");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("new_id")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(newIdMeta),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(flagMeta)))),
              new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("flag")));
      assertThat(r.getMetadata().getColumns().getFields())
          .extracting(DeltaStructField::getName)
          .containsExactly("new_id", "flag");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("flag");
    }

    // -------- set-partition-columns references unknown column --------
    {
      Handle h = createDeltaExternal("tbl_setpart_bad");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("nope"))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "partition-columns references unknown column: nope");
    }

    // -------- set-columns with no columns block is rejected --------
    {
      Handle h = createDeltaExternal("tbl_setcols_no_block");
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetSchemaUpdate()),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-columns requires a columns block");
    }

    // -------- set-columns with an empty fields list is rejected --------
    {
      Handle h = createDeltaExternal("tbl_setcols_empty_fields");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h, new DeltaSetSchemaUpdate().columns(new DeltaStructType().fields(List.of()))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-columns requires at least one column");
    }

    // -------- set-partition-columns with a null partition-columns field is rejected --------
    // The Java client model defaults the field to an empty list, so an explicit null is needed
    // to exercise the wire-level "field missing" case (Jackson NON_NULL inclusion omits it).
    {
      Handle h = createDeltaExternal("tbl_setpart_null_field");
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(null)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-partition-columns requires a partition-columns list");
    }

    // -------- set-partition-columns alone --------
    // The partition-only branch of the unified schema/partition path: the existing schema is
    // carried over (with partition indices cleared) and re-stamped with the requested partition
    // list. Verify the partition is set and the column list itself is unchanged.
    {
      Handle h = createDeltaExternal("tbl_setpart_only");
      DeltaLoadTableResponse r =
          updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("amount")));
      assertThat(r.getMetadata().getColumns().getFields())
          .extracting(DeltaStructField::getName)
          .containsExactly("id", "amount");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("amount");
    }

    // -------- set-columns alone preserves existing partition columns by name --------
    // Establish a partitioned table partitioned by id, then send set-columns alone with a new
    // schema that still contains id (plus a new "flag" column). Partition list must survive the
    // schema swap. physicalName "id" == current logical "id": first-enable without rename for id.
    // physicalName "flag" has no current match: new column, no rename.
    {
      Handle h = createDeltaExternal("tbl_setcols_preserve_part");
      DeltaLoadTableResponse partitionSetup =
          updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("id")));
      Handle h1 = h.withEtag(partitionSetup.getMetadata().getEtag());
      DeltaStructFieldMetadata idCmMeta = new DeltaStructFieldMetadata();
      idCmMeta.put("delta.columnMapping.id", 1);
      idCmMeta.put("delta.columnMapping.physicalName", "id");
      DeltaStructFieldMetadata flagCmMeta = new DeltaStructFieldMetadata();
      flagCmMeta.put("delta.columnMapping.id", 2);
      flagCmMeta.put("delta.columnMapping.physicalName", "flag");
      DeltaLoadTableResponse r =
          updateTable(
              h1,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("id")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(idCmMeta),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(flagCmMeta)))));
      assertThat(r.getMetadata().getColumns().getFields())
          .extracting(DeltaStructField::getName)
          .containsExactly("id", "flag");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("id");
    }

    // -------- set-columns alone that drops a previously-partition column is rejected --------
    // Partition is on `id`; the new schema replaces `id` with `id2` (no set-partition-columns
    // sent).
    // physicalName "new-id2" doesn't match any current name, so no first-enable rename fires.
    // The partition check then fails: "partition-columns references unknown column: id".
    {
      Handle h = createDeltaExternal("tbl_setcols_drop_part");
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("id")))
                  .getMetadata()
                  .getEtag());
      DeltaStructFieldMetadata id2Meta = new DeltaStructFieldMetadata();
      id2Meta.put("delta.columnMapping.id", 1);
      id2Meta.put("delta.columnMapping.physicalName", "new-id2");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h1,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("id2")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(id2Meta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "partition-columns references unknown column: id");
    }

    // -------- assert-etag conflict: mutate once to roll the etag, then resubmit with the stale
    // value pinned explicitly so the conflict path actually fires.
    {
      Handle h = createDeltaManaged("tbl_etag_conflict", Map.of());
      updateTable(h, new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")));
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h.name(),
                  requestWith(
                      h.tableId(),
                      Optional.of(h.etag()),
                      new DeltaSetPropertiesUpdate().updates(Map.of("x", "y")))),
          DeltaErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION,
          "assert-etag failed");
    }

    // -------- request-shape rejections (share one table; none mutate state) --------
    {
      Handle h = createDeltaManaged("tbl_rejects", Map.of());
      DeltaSetPropertiesUpdate setKv = new DeltaSetPropertiesUpdate().updates(Map.of("k", "v"));

      // Missing assert-table-uuid: rejected during collectRequest.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h.name(),
                  new DeltaUpdateTableRequest().requirements(List.of()).updates(List.of(setKv))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "assert-table-uuid requirement is required");

      // assert-table-uuid mismatch: rejected during checkTableUuidRequirement.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h.name(), requestWith(UUID.randomUUID(), setKv)),
          DeltaErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION,
          "assert-table-uuid failed");

      // Two same-typed actions in one request.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetPropertiesUpdate().updates(Map.of("a", "1")),
                  new DeltaSetPropertiesUpdate().updates(Map.of("b", "2"))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "At most one set-properties is allowed per request");

      // set-properties and remove-properties touching the same key.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, setKv, new DeltaRemovePropertiesUpdate().removals(List.of("k"))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-properties and remove-properties overlap");

      // set-domain-metadata and remove-domain-metadata touching the same domain.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetDomainMetadataUpdate()
                      .updates(
                          new DeltaDomainMetadataUpdates()
                              .deltaRowTracking(
                                  new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(1L))),
                  new DeltaRemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking"))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-domain-metadata and remove-domain-metadata overlap");

      // Empty updates list.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h.name(), requestWith(h.tableId())),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "At least one update is required");

      // Path resolution fails before the UUID is checked, so the synthesized random UUID is fine.
      TestUtils.assertDeltaApiException(
          () -> updateTable("no_such_table", requestWith(UUID.randomUUID(), setKv)),
          DeltaErrorType.NO_SUCH_TABLE_EXCEPTION,
          "Table not found");

      // set-protocol with null protocol -- rejected in applyUpdates.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetProtocolUpdate().protocol(null)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-protocol requires a protocol");

      // set-domain-metadata with null updates block -- rejected in applyUpdates.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetDomainMetadataUpdate().updates(null)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-domain-metadata requires an updates block");

      // set-domain-metadata with a non-null but empty block -- silent no-op, reject loudly.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h, new DeltaSetDomainMetadataUpdate().updates(new DeltaDomainMetadataUpdates())),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-domain-metadata requires at least one domain entry");

      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetTableCommentUpdate().comment(null)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-table-comment requires a comment");

      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h, new DeltaRemoveDomainMetadataUpdate().domains(List.of("delta.unknown"))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Unknown domain in remove-domain-metadata");

      // set-protocol on MANAGED must keep every catalog-managed required feature.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetProtocolUpdate()
                      .protocol(
                          new DeltaProtocol()
                              .minReaderVersion(3)
                              .minWriterVersion(7)
                              .readerFeatures(List.of(TableFeature.V2_CHECKPOINT.specName()))
                              .writerFeatures(List.of(TableFeature.V2_CHECKPOINT.specName())))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "catalogManaged");

      // set-protocol that satisfies all required features but drops rowTracking, while the table
      // still carries delta.rowTracking.rowIdHighWaterMark from create-time seeding. The
      // post-update validation synthesizes the effective domain metadata from properties and
      // catches the missing writer feature.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetProtocolUpdate()
                      .protocol(
                          new DeltaProtocol()
                              .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                              .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                              .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                              .writerFeatures(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "rowTracking");
    }

    // -------- set-protocol on EXTERNAL with a non-contract protocol is allowed --------
    // EXTERNAL tables are not subject to the catalog-managed contract; pins that the MANAGED
    // re-validation guard doesn't over-block.
    {
      Handle external = createDeltaExternal("tbl_setproto_external");
      DeltaLoadTableResponse r =
          updateTable(
              external,
              new DeltaSetProtocolUpdate()
                  .protocol(
                      new DeltaProtocol()
                          .minReaderVersion(3)
                          .minWriterVersion(7)
                          .readerFeatures(List.of(TableFeature.V2_CHECKPOINT.specName()))
                          .writerFeatures(List.of(TableFeature.V2_CHECKPOINT.specName()))));
      // Post-update feature set is exactly what was sent; no catalog-managed features injected.
      Map<String, String> props = r.getMetadata().getProperties();
      assertThat(props).containsEntry(TableProperties.FEATURE_PREFIX + "v2Checkpoint", "supported");
      assertThat(props).doesNotContainKey(TableProperties.FEATURE_PREFIX + "catalogManaged");
    }

    // -------- non-Delta table is rejected before any mutation commits --------
    {
      TableInfo external =
          createTestingTable(
              "tbl_external_parquet",
              TableType.EXTERNAL,
              Optional.of(testDirectoryRoot.toString()),
              DataSourceFormat.PARQUET,
              tableOperations);
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  external.getName(),
                  requestWith(
                      UUID.fromString(external.getTableId()),
                      new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")))),
          DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION,
          "Table is not a Delta table");
    }

    // -------- add-commit on managed Delta (+ version conflict) --------
    {
      Handle h = createDeltaManaged("tbl_commit", Map.of());
      Long t0 =
          deltaTablesApi
              .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name())
              .getMetadata()
              .getUpdatedTime();
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      assertThat(r1.getCommits()).hasSize(1);
      assertThat(r1.getCommits().get(0).getVersion()).isEqualTo(1L);
      assertThat(r1.getLatestTableVersion()).isEqualTo(1L);
      // A data-only commit changes no metadata, so it must not advance updated-time (and hence
      // must not roll the etag). The conflict calls below reuse this etag in assert-etag and
      // fail on the version check, which also proves the pre-commit etag remains valid.
      assertThat(r1.getMetadata().getUpdatedTime()).isEqualTo(t0);
      assertThat(r1.getMetadata().getEtag()).isEqualTo(h.etag());

      Handle h1 = h.withEtag(r1.getMetadata().getEtag());

      // UUID-based idempotency: replaying the identical v1 add-commit (same file name / UUID)
      // must succeed as a no-op rather than conflict, so a client that lost the original response
      // is not misled into rebasing and duplicating the commit. The table state is unchanged.
      DeltaLoadTableResponse rReplay =
          updateTable(
              h1,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      assertThat(rReplay.getCommits()).hasSize(1);
      assertThat(rReplay.getCommits().get(0).getVersion()).isEqualTo(1L);
      assertThat(rReplay.getCommits().get(0).getFileName()).isEqualTo("00000001.json");
      assertThat(rReplay.getLatestTableVersion()).isEqualTo(1L);
      assertThat(rReplay.getMetadata().getEtag()).isEqualTo(r1.getMetadata().getEtag());

      // Replaying v1 with a DIFFERENT file name means another writer won that version: this must
      // surface as a CommitVersionConflict (409). Pin assert-etag against the post-r1 etag so the
      // conflict comes from the version check, not from assert-etag.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h1,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000002L)
                              .fileName("00000001b.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000002L))),
          DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION,
          "already accepted");

      // Rejecting a v3 while the table is on v1 -- must be v1+1.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h1,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(3L)
                              .timestamp(1700000003L)
                              .fileName("00000003.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000003L))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "next version");
    }

    // -------- idempotent replay of non-newest tracked versions (first-DAO + DB-lookup branches) --
    {
      Handle h = createDeltaManaged("tbl_commit_idempotent_multi", Map.of());
      Handle cur = h;
      // Build up three tracked, unbackfilled commits: v1, v2, v3.
      for (long v = 1; v <= 3; v++) {
        DeltaLoadTableResponse r =
            updateTable(
                cur,
                new DeltaAddCommitUpdate()
                    .commit(
                        new DeltaCommit()
                            .version(v)
                            .timestamp(1700000000L + v)
                            .fileName(String.format("%08d.json", v))
                            .fileSize(1024L)
                            .fileModificationTimestamp(1700000000L + v)));
        cur = cur.withEtag(r.getMetadata().getEtag());
      }
      final Handle h3 = cur;

      // Replay the OLDEST tracked version (v1) with its original file name -> idempotent 200.
      DeltaLoadTableResponse replayFirst =
          updateTable(
              h3,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      assertThat(replayFirst.getLatestTableVersion()).isEqualTo(3L);
      assertThat(replayFirst.getCommits()).hasSize(3);

      // Replay a MIDDLE tracked version (v2) with its original file name -> idempotent 200. This
      // is the branch that resolves the existing commit via a DB lookup rather than a boundary DAO.
      DeltaLoadTableResponse replayMiddle =
          updateTable(
              h3,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(2L)
                          .timestamp(1700000002L)
                          .fileName("00000002.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000002L)));
      assertThat(replayMiddle.getLatestTableVersion()).isEqualTo(3L);
      assertThat(replayMiddle.getCommits()).hasSize(3);

      // A middle version with a DIFFERENT file name is still a genuine conflict.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h3,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(2L)
                              .timestamp(1700000002L)
                              .fileName("00000002_other.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000002L))),
          DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION,
          "already accepted");
    }

    // -------- assert-etag is skipped for a verified commit replay, enforced otherwise --------
    {
      Handle h = createDeltaManaged("tbl_commit_idempotent_meta", Map.of());
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)),
              new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")));
      // The metadata change rolled the etag, so h.etag() is now stale.
      assertThat(r1.getMetadata().getEtag()).isNotEqualTo(h.etag());
      Handle hStale = h; // pins the stale (pre-commit) etag

      // Verified replay with the STALE etag pinned: recognized as an already-accepted commit, so
      // the whole request is a 200 no-op (the stale etag does not conflict) and the table is
      // unchanged.
      DeltaLoadTableResponse replayStaleEtag =
          updateTable(
              hStale,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)),
              new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")));
      assertThat(replayStaleEtag.getLatestTableVersion()).isEqualTo(1L);
      assertThat(replayStaleEtag.getCommits()).hasSize(1);
      assertThat(replayStaleEtag.getMetadata().getProperties()).containsEntry("k", "v");

      // A genuine NEW commit (v2) with the STALE etag is NOT a replay, so assert-etag is enforced
      // and the stale etag correctly fails -- optimistic concurrency still protects real writes.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  hStale,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(2L)
                              .timestamp(1700000002L)
                              .fileName("00000002.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000002L))),
          DeltaErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION,
          "assert-etag failed");

      // The same NEW v2 commit with the CURRENT etag proceeds normally (table still at v1). The
      // replay above was a no-op, so the etag is unchanged from r1.
      Handle hFresh = h.withEtag(replayStaleEtag.getMetadata().getEtag());
      DeltaLoadTableResponse rNext =
          updateTable(
              hFresh,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(2L)
                          .timestamp(1700000002L)
                          .fileName("00000002.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000002L)));
      assertThat(rNext.getLatestTableVersion()).isEqualTo(2L);
    }

    // ------- a commit replay is a whole-request no-op, even with a stale etag and extra actions --
    // The client's original v2 attempt (commit-only) landed but the response was lost. The retry
    // resends the identical v2 commit, pins the now-stale (pre-v2) etag, and folds in a new
    // backfill of v1.
    // Because the commit is a recognized replay, the whole request is a no-op success: the stale
    // etag does not conflict, and the extra backfill is NOT applied (v1 is retained). To report
    // backfill after a lost response, the client sends a standalone set-latest-backfilled-version.
    {
      Handle h = createDeltaManaged("tbl_replay_whole_request_noop", Map.of());
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      // Data-only commit does not roll the etag.
      Handle afterV1 = h.withEtag(r1.getMetadata().getEtag());
      updateTable(
          afterV1,
          new DeltaAddCommitUpdate()
              .commit(
                  new DeltaCommit()
                      .version(2L)
                      .timestamp(1700000002L)
                      .fileName("00000002.json")
                      .fileSize(1024L)
                      .fileModificationTimestamp(1700000002L)));

      // Replay v2 (same file name) + report backfill of v1, pinning the now-stale afterV1 etag.
      DeltaLoadTableResponse replay =
          updateTable(
              afterV1,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(2L)
                          .timestamp(1700000002L)
                          .fileName("00000002.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000002L)),
              new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(1L));
      // Whole-request no-op: no conflict from the stale etag, and the backfill did NOT run, so both
      // v1 and v2 remain unbackfilled.
      assertThat(replay.getLatestTableVersion()).isEqualTo(2L);
      assertThat(replay.getCommits()).hasSize(2);
    }

    // -------- add-commit + set-latest-backfilled-version in one request --------
    // Exercises the combined commit+backfill path that reads getFirstAndLastCommits once for
    // both actions, matching the UC REST postCommit behavior on a combined request.
    {
      Handle h = createDeltaManaged("tbl_commit_backfill", Map.of());
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      assertThat(r1.getLatestTableVersion()).isEqualTo(1L);

      // Now send v2 commit + backfill of v1 in a single request.
      DeltaLoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(2L)
                          .timestamp(1700000002L)
                          .fileName("00000002.json")
                          .fileSize(2048L)
                          .fileModificationTimestamp(1700000002L)),
              new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(1L));
      // v1 was removed by the backfill; only v2 remains in the unbackfilled set.
      assertThat(r2.getLatestTableVersion()).isEqualTo(2L);
      assertThat(r2.getCommits()).hasSize(1);
      assertThat(r2.getCommits().get(0).getVersion()).isEqualTo(2L);
      // The combined commit+backfill request is still data-only: updated-time must not advance.
      assertThat(r2.getMetadata().getUpdatedTime()).isEqualTo(r1.getMetadata().getUpdatedTime());
    }

    // -------- MANAGED add-commit + metadata change stamps lastUpdateVersion/timestamp --------
    // A metadata-changing commit on a MANAGED table should update delta.lastUpdateVersion and
    // delta.lastCommitTimestamp to the commit's version / timestamp, without the client having
    // to send them in set-properties. A data-only commit (no metadata change in the request)
    // leaves those unchanged.
    {
      Handle h = createDeltaManaged("tbl_commit_meta_stamp", Map.of());

      // v1: add-commit + set-properties (metadata-changing) -> stamps both props.
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)),
              new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")));
      assertThat(r1.getMetadata().getLastCommitVersion()).isEqualTo(1L);
      assertThat(r1.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000001L);

      // v2: add-commit with no metadata change -> previous values preserved.
      DeltaLoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(2L)
                          .timestamp(1700000002L)
                          .fileName("00000002.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000002L)));
      assertThat(r2.getMetadata().getLastCommitVersion()).isEqualTo(1L);
      assertThat(r2.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000001L);

      // v3: add-commit + set-domain-metadata touching only delta.rowTracking. The Delta protocol
      // makes writers mirror the row-tracking high-water mark on every fresh-row commit, so this
      // is data-stream bookkeeping: the HWM property must persist, but lastUpdateVersion,
      // updated-time, and the etag must all stay where the last real metadata change (v1) left
      // them.
      DeltaLoadTableResponse r3 =
          updateTable(
              h.withEtag(r2.getMetadata().getEtag()),
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(3L)
                          .timestamp(1700000003L)
                          .fileName("00000003.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000003L)),
              new DeltaSetDomainMetadataUpdate()
                  .updates(
                      new DeltaDomainMetadataUpdates()
                          .deltaRowTracking(
                              new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(150L))));
      assertThat(r3.getMetadata().getProperties())
          .containsEntry(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK, "150");
      assertThat(r3.getMetadata().getLastCommitVersion()).isEqualTo(1L);
      assertThat(r3.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000001L);
      assertThat(r3.getMetadata().getUpdatedTime()).isEqualTo(r1.getMetadata().getUpdatedTime());
      assertThat(r3.getMetadata().getEtag()).isEqualTo(r1.getMetadata().getEtag());
    }

    // -------- add-commit with no commit block is rejected --------
    {
      Handle h = createDeltaManaged("tbl_commit_no_block", Map.of());
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaAddCommitUpdate()),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "add-commit requires a commit block");
    }

    // -------- set-latest-backfilled-version with no version is rejected --------
    {
      Handle h = createDeltaManaged("tbl_backfill_no_version", Map.of());
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new DeltaSetLatestBackfilledVersionUpdate()),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-latest-backfilled-version requires latest-published-version");
    }

    // -------- add-commit on EXTERNAL rejected --------
    {
      Handle h = createDeltaExternal("tbl_ext_commit");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "MANAGED");
    }

    // -------- add-commit + uniform: mismatched version rejected --------
    {
      Handle h = createDeltaManaged("tbl_uniform_bad", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L))
                      .uniform(
                          new DeltaUniformMetadata()
                              .iceberg(
                                  new DeltaUniformMetadataIceberg()
                                      .metadataLocation("file:///tmp/ice/v2.json")
                                      .convertedDeltaVersion(2L)
                                      .convertedDeltaTimestamp(1700000001L)))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "converted-delta-version");
    }

    // -------- set-latest-backfilled-version alone on a table with prior commits --------
    // Exercises the standalone backfill-only branch that the combined add-commit + backfill test
    // doesn't reach (that one goes through handleNormalCommit). The post-backfill commits list
    // isn't asserted: markCommitAsLatestBackfilled issues a native SQL UPDATE that the session
    // cache doesn't reflect in the same-transaction read used to build the response.
    {
      Handle h = createDeltaManaged("tbl_backfill_only", Map.of());
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      assertThat(r1.getLatestTableVersion()).isEqualTo(1L);
      DeltaLoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(1L));
      assertThat(r2.getLatestTableVersion()).isEqualTo(1L);
      // A backfill notification is not a metadata change, so it must not advance updated-time
      // (and hence must not roll the etag).
      assertThat(r2.getMetadata().getUpdatedTime()).isEqualTo(r1.getMetadata().getUpdatedTime());
      assertThat(r2.getMetadata().getEtag()).isEqualTo(r1.getMetadata().getEtag());
    }

    // -------- set-latest-backfilled-version on a table with no prior commits rejected --------
    {
      Handle h = createDeltaManaged("tbl_backfill_empty", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h, new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(1L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Backfill request requires a prior commit");
    }

    // -------- set-latest-backfilled-version past the last commit rejected --------
    {
      Handle h = createDeltaManaged("tbl_backfill_past_last", Map.of());
      DeltaLoadTableResponse r1 =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)));
      Handle h1 = h.withEtag(r1.getMetadata().getEtag());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h1, new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(5L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Should not backfill version 5");
    }

    // -------- add-commit + uniform with metadata-location outside the table location rejected --
    // Pins the subpath check on the Delta update path, mirroring the create-time check. The
    // sibling set-properties enables uniform.iceberg so the consistency check passes and the
    // subpath check is reached.
    {
      Handle h = createDeltaManaged("tbl_uniform_off_root", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetPropertiesUpdate()
                      .updates(Map.of("delta.universalFormat.enabledFormats", "iceberg")),
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L))
                      .uniform(
                          new DeltaUniformMetadata()
                              .iceberg(
                                  new DeltaUniformMetadataIceberg()
                                      .metadataLocation("s3://test-bucket0/elsewhere/v1.json")
                                      .convertedDeltaVersion(1L)
                                      .convertedDeltaTimestamp(1700000001L)))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "must be a subpath");
    }

    // -------- add-commit + uniform block on a table without iceberg enabled is rejected --------
    {
      Handle h = createDeltaManaged("tbl_uniform_block_no_prop", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L))
                      .uniform(
                          new DeltaUniformMetadata()
                              .iceberg(
                                  new DeltaUniformMetadataIceberg()
                                      .metadataLocation("s3://test-bucket0/path/v1.json")
                                      .convertedDeltaVersion(1L)
                                      .convertedDeltaTimestamp(1700000001L)))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Uniform metadata must not be set unless");
    }

    // -------- set-properties enabling iceberg + add-commit WITHOUT uniform block is rejected --
    {
      Handle h = createDeltaManaged("tbl_prop_no_uniform_block", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetPropertiesUpdate()
                      .updates(Map.of("delta.universalFormat.enabledFormats", "iceberg")),
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Uniform metadata must be set when");
    }

    // -------- set-properties + add-commit + valid uniform (subpath + version match) succeeds --
    {
      Handle h = createDeltaManaged("tbl_uniform_ok", Map.of());
      // Handle doesn't carry the staging location; loadTable to get it so we can build a
      // metadata-location that satisfies the subpath check.
      String tableLocation =
          deltaTablesApi
              .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name())
              .getMetadata()
              .getLocation();
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetPropertiesUpdate()
                  .updates(Map.of("delta.universalFormat.enabledFormats", "iceberg")),
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L))
                  .uniform(
                      new DeltaUniformMetadata()
                          .iceberg(
                              new DeltaUniformMetadataIceberg()
                                  .metadataLocation(tableLocation + "/_uniform/v1.json")
                                  .convertedDeltaVersion(1L)
                                  .convertedDeltaTimestamp(1700000001L))));
      assertThat(r.getCommits()).hasSize(1);
      assertThat(r.getCommits().get(0).getVersion()).isEqualTo(1L);
    }

    // -------- add-commit + set-schema stamps lastUpdateVersion (non-set-properties path) --
    // set-schema alone is a metadata change, so hasManagedTableMetadataChange() must fire and the
    // commit's version/timestamp should land on the stamping properties.
    // physicalName "c1" doesn't match any current column name → first-enable without rename.
    {
      Handle h = createDeltaManaged("tbl_commit_meta_stamp_schema", Map.of());
      DeltaStructFieldMetadata c1Meta = new DeltaStructFieldMetadata();
      c1Meta.put("delta.columnMapping.id", 1);
      c1Meta.put("delta.columnMapping.physicalName", "c1");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)),
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("c1")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(c1Meta)))));
      assertThat(r.getMetadata().getLastCommitVersion()).isEqualTo(1L);
      assertThat(r.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000001L);
    }

    // -------- add-commit + update-metadata-snapshot-version is rejected on MANAGED --
    // update-metadata-snapshot-version is EXTERNAL-only; bundling it with add-commit (MANAGED-only)
    // never succeeds. Pins that the EXTERNAL check fires before prepareCommitAndBackfill so
    // hasManagedTableMetadataChange can safely omit updateSnapshotVersion from its predicate.
    {
      Handle h = createDeltaManaged("tbl_commit_with_snapshot", Map.of());
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaAddCommitUpdate()
                      .commit(
                          new DeltaCommit()
                              .version(1L)
                              .timestamp(1700000001L)
                              .fileName("00000001.json")
                              .fileSize(1024L)
                              .fileModificationTimestamp(1700000001L)),
                  new DeltaUpdateSnapshotVersionUpdate()
                      .lastCommitVersion(1L)
                      .lastCommitTimestampMs(1700000001L)),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "only supported for EXTERNAL");
    }

    // -------- add-commit + set-protocol stamps lastUpdateVersion (a second hasManagedTable-
    // MetadataChange arm exercised alongside the set-schema case above).
    {
      Handle h = createDeltaManaged("tbl_commit_meta_stamp_protocol", Map.of());
      List<String> writerFeatures =
          new ArrayList<>(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES);
      writerFeatures.add(TableFeature.ROW_TRACKING.specName());
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaAddCommitUpdate()
                  .commit(
                      new DeltaCommit()
                          .version(1L)
                          .timestamp(1700000001L)
                          .fileName("00000001.json")
                          .fileSize(1024L)
                          .fileModificationTimestamp(1700000001L)),
              new DeltaSetProtocolUpdate()
                  .protocol(
                      new DeltaProtocol()
                          .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                          .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                          .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                          .writerFeatures(writerFeatures)));
      assertThat(r.getMetadata().getLastCommitVersion()).isEqualTo(1L);
      assertThat(r.getMetadata().getLastCommitTimestampMs()).isEqualTo(1700000001L);
    }

    // -------- content-based idempotency for a backfilled-and-purged version --------
    // Once a version is backfilled and purged, its staged file name is no longer tracked, so a
    // replay is settled by comparing the incoming staged file against the published commit file.
    {
      DeltaStagingTableResponse staging = createDeltaStaging("tbl_content_idem_purged");
      Handle h = createDeltaManaged("tbl_content_idem_purged", staging, Map.of());
      String loc = staging.getLocation();
      // Commit v1, v2, v3, keeping each update so the v2 object can be replayed verbatim below.
      DeltaAddCommitUpdate[] commits = new DeltaAddCommitUpdate[3];
      Handle cur = h;
      for (int i = 0; i < commits.length; i++) {
        long v = i + 1;
        commits[i] =
            new DeltaAddCommitUpdate()
                .commit(
                    new DeltaCommit()
                        .version(v)
                        .timestamp(1700000000L + v)
                        .fileName(String.format("%08d.json", v))
                        .fileSize(1024L)
                        .fileModificationTimestamp(1700000000L + v));
        cur = cur.withEtag(updateTable(cur, commits[i]).getMetadata().getEtag());
      }
      // Backfill v2 -> purge v1 and v2 from the DB (leaving [v3]).
      DeltaLoadTableResponse afterBackfill =
          updateTable(cur, new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(2L));
      final Handle h3 = cur.withEtag(afterBackfill.getMetadata().getEtag());

      // Publish v2 and stage a byte-identical file: the replay is recognized by content and is an
      // idempotent no-op success (table still at v3).
      byte[] v2Content = "delta-commit-v2\n".getBytes(StandardCharsets.UTF_8);
      writeTableFile(loc, "_delta_log/00000000000000000002.json", v2Content);
      writeTableFile(loc, "_delta_log/_staged_commits/00000002.json", v2Content);
      DeltaLoadTableResponse replay = updateTable(h3, commits[1]);
      assertThat(replay.getLatestTableVersion()).isEqualTo(3L);

      // A staged file whose content differs from the published commit is a definitive conflict.
      writeTableFile(
          loc,
          "_delta_log/_staged_commits/00000002.json",
          "different\n".getBytes(StandardCharsets.UTF_8));
      TestUtils.assertDeltaApiException(
          () -> updateTable(h3, commits[1]),
          DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION,
          "already accepted");

      // The published file is absent (deleted here): the outcome cannot be determined, so the whole
      // request fails open to a retriable COMMIT_STATE_UNKNOWN (500), never a false conflict.
      deleteTablePath(loc, "_delta_log/00000000000000000002.json");
      TestUtils.assertDeltaApiException(
          () -> updateTable(h3, commits[1]),
          DeltaErrorType.COMMIT_STATE_UNKNOWN_EXCEPTION,
          "retry");

      // The published path exists but cannot be read as a file (a directory stands in for a
      // permission / non-not-found IO error): still COMMIT_STATE_UNKNOWN, not a false conflict.
      createTableDir(loc, "_delta_log/00000000000000000002.json");
      TestUtils.assertDeltaApiException(
          () -> updateTable(h3, commits[1]),
          DeltaErrorType.COMMIT_STATE_UNKNOWN_EXCEPTION,
          "retry");
    }
  }

  /** Write {@code content} to {@code relativePath} under the table's (file://) storage location. */
  private void writeTableFile(String storageLocation, String relativePath, byte[] content)
      throws IOException {
    Path path = Path.of(URI.create(storageLocation + "/" + relativePath));
    Files.createDirectories(path.getParent());
    Files.write(path, content);
  }

  /** Delete {@code relativePath} under the table's (file://) storage location. */
  private void deleteTablePath(String storageLocation, String relativePath) throws IOException {
    Files.delete(Path.of(URI.create(storageLocation + "/" + relativePath)));
  }

  /** Create {@code relativePath} as a directory under the table's (file://) storage location. */
  private void createTableDir(String storageLocation, String relativePath) throws IOException {
    Files.createDirectories(Path.of(URI.create(storageLocation + "/" + relativePath)));
  }

  // ---------------------------------------------------------------- column-mapping identity tests

  /**
   * After a set-columns rename (logical "a" → "b", same physicalName "col-1"), the rename succeeds:
   * the new logical name is present, the old is gone, and physicalName is retained in the column's
   * type_json metadata. Database row UUIDs are regenerated on every set-columns (standard catalog
   * semantics); no UUID-preservation assertion is made here.
   */
  @Test
  public void testRenameColumnNameMode() throws Exception {
    Handle h = createCmNameModeManaged("tbl_rename_name_mode");
    DeltaLoadTableResponse r = updateTable(h, setColumnsRenaming("a", "b", "col-1"));
    assertThat(columnNames(r)).containsExactly("b");
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("b");
    // physicalName must survive the rename.
    DeltaStructFieldMetadata meta =
        reloaded.getMetadata().getColumns().getFields().get(0).getMetadata();
    assertThat(meta.get("delta.columnMapping.physicalName")).isEqualTo("col-1");
  }

  /**
   * set-columns that retains a column by logical name but strips its column-mapping identity key
   * (present in the existing schema, absent in the incoming schema) must be rejected. Uses a
   * two-column table: "a" retains its CM key, "b" loses it, so the incoming schema still has CM
   * metadata overall (preventing the full-CM-drop check from firing first). The identity-strip
   * check fires for "b" as the offending column.
   */
  @Test
  public void testSetColumnsDroppingColumnMappingMetadataIsRejected() throws Exception {
    Handle h = createTwoColCmNameModeManaged("tbl_drop_cm_meta");
    // Send "a" with CM intact, "b" with CM stripped.
    DeltaStructFieldMetadata aMetaOk = new DeltaStructFieldMetadata();
    aMetaOk.put("delta.columnMapping.id", 1);
    aMetaOk.put("delta.columnMapping.physicalName", "col-1");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                h,
                new DeltaSetSchemaUpdate()
                    .columns(
                        new DeltaStructType()
                            .type("struct")
                            .fields(
                                List.of(
                                    new DeltaStructField()
                                        .name("a")
                                        .type(new DeltaPrimitiveType().type("long"))
                                        .nullable(false)
                                        .metadata(aMetaOk),
                                    new DeltaStructField()
                                        .name("b")
                                        .type(new DeltaPrimitiveType().type("string"))
                                        .nullable(true)
                                        .metadata(new DeltaStructFieldMetadata()))))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): b");
  }

  /**
   * Renaming a nested field "x" → "y" inside a top-level struct column "s" (outer logical name and
   * physicalName unchanged) succeeds: the outer column "s" is still present with the same outer
   * physicalName; the nested field appears as "y" inside the struct type. DB-row UUIDs are
   * regenerated on every set-columns; no UUID assertion is made here.
   */
  @Test
  public void testNestedFieldRename() throws Exception {
    Handle h = createCmNameModeStructManaged("tbl_nested_field_rename");
    // Rename the nested field "x" to "y" while keeping top-level "s" and physicalNames unchanged.
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-x");
    DeltaStructType newInnerType =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("y")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-s");
    updateTable(
        h,
        new DeltaSetSchemaUpdate()
            .columns(
                new DeltaStructType()
                    .type("struct")
                    .fields(
                        List.of(
                            new DeltaStructField()
                                .name("s")
                                .type(newInnerType)
                                .nullable(false)
                                .metadata(outerMeta)))));
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("s");
    // Outer physicalName must survive the nested rename.
    DeltaStructFieldMetadata outerResult =
        reloaded.getMetadata().getColumns().getFields().get(0).getMetadata();
    assertThat(outerResult.get("delta.columnMapping.physicalName")).isEqualTo("col-s");
  }

  /**
   * Renaming a field inside a {@code map<string, struct<...>>} value struct ("val" → "value")
   * succeeds as a nested type_json passthrough: the top-level column "props" is retained with its
   * outer physicalName, and the nested rename rides inside the value struct. Exercises the map arm
   * of the recursive rename/consistency checks.
   */
  @Test
  public void testMapStructNestedFieldRename() throws Exception {
    Handle h = createCmNameModeMapStructManaged("tbl_map_struct_rename");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-val");
    DeltaStructType newValueStruct =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("value")
                        .type(new DeltaPrimitiveType().type("string"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-props");
    updateTable(
        h,
        new DeltaSetSchemaUpdate()
            .columns(
                new DeltaStructType()
                    .type("struct")
                    .fields(
                        List.of(
                            new DeltaStructField()
                                .name("props")
                                .type(
                                    new DeltaMapType()
                                        .type("map")
                                        .keyType(new DeltaPrimitiveType().type("string"))
                                        .valueType(newValueStruct)
                                        .valueContainsNull(true))
                                .nullable(true)
                                .metadata(outerMeta)))));
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("props");
    assertThat(
            reloaded
                .getMetadata()
                .getColumns()
                .getFields()
                .get(0)
                .getMetadata()
                .get("delta.columnMapping.physicalName"))
        .isEqualTo("col-props");
  }

  /**
   * Renaming a field inside an {@code array<struct<...>>} element struct ("label" -> "tag")
   * succeeds as a nested type_json passthrough: the top-level column "events" is retained with its
   * outer physicalName, and the nested rename rides inside the element struct. Exercises the array
   * arm of the recursive identity handling.
   */
  @Test
  public void testArrayStructNestedFieldRename() throws Exception {
    Handle h = createCmNameModeArrayStructManaged("tbl_array_struct_rename");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-label");
    DeltaStructType newElementStruct =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("tag")
                        .type(new DeltaPrimitiveType().type("string"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-events");
    updateTable(
        h,
        new DeltaSetSchemaUpdate()
            .columns(
                new DeltaStructType()
                    .type("struct")
                    .fields(
                        List.of(
                            new DeltaStructField()
                                .name("events")
                                .type(
                                    new DeltaArrayType()
                                        .type("array")
                                        .elementType(newElementStruct)
                                        .containsNull(true))
                                .nullable(true)
                                .metadata(outerMeta)))));
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("events");
    assertThat(
            reloaded
                .getMetadata()
                .getColumns()
                .getFields()
                .get(0)
                .getMetadata()
                .get("delta.columnMapping.physicalName"))
        .isEqualTo("col-events");
  }

  /**
   * Top-level rename on an id-mode table (CM identity keyed on {@code delta.columnMapping.id}).
   * After "a" → "b" the new logical name is present, old is gone, and the CM id remains 1 in the
   * column's type_json. DB-row UUIDs are regenerated on every set-columns.
   */
  @Test
  public void testRenameColumnIdMode() throws Exception {
    Handle h = createCmIdModeManaged("tbl_rename_id_mode");
    DeltaLoadTableResponse r = updateTable(h, setColumnsRenamingIdMode("a", "b", 1, "col-1"));
    assertThat(columnNames(r)).containsExactly("b");
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("b");
    // CM id must survive the rename in type_json.
    DeltaStructFieldMetadata meta =
        reloaded.getMetadata().getColumns().getFields().get(0).getMetadata();
    assertThat(meta.get("delta.columnMapping.id")).isEqualTo(1);
  }

  // ---------------------------------------------------------------- rename guards + support matrix

  /**
   * Verifies that an unknown {@code delta.columnMapping.mode} value (e.g. "foo") stored on a table
   * causes the set-columns guard to reject the request with "Invalid column mapping mode".
   *
   * <p>The mode is seeded by setting the raw property via {@code set-properties} on an external
   * table (which accepts arbitrary properties), then sending {@code set-schema} — at that point the
   * mode is read back from the stored property map and the guard fires.
   */
  @Test
  public void testSetColumnsRejectsInvalidColumnMappingMode() throws Exception {
    Handle h = createDeltaExternal("tbl_invalid_cm_mode");
    // Seed an invalid mode. set-properties runs before the schema guard so it persists first.
    updateTable(
        h, new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", "foo")));
    Handle h2 =
        h.withEtag(
            deltaTablesApi
                .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name())
                .getMetadata()
                .getEtag());
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    meta.put("delta.columnMapping.id", 1);
    meta.put("delta.columnMapping.physicalName", "id");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                h2,
                new DeltaSetSchemaUpdate()
                    .columns(
                        new DeltaStructType()
                            .type("struct")
                            .fields(
                                List.of(
                                    new DeltaStructField()
                                        .name("id")
                                        .type(new DeltaPrimitiveType().type("long"))
                                        .nullable(false)
                                        .metadata(meta))))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "Invalid column mapping mode");
  }

  /**
   * Comprehensive test for all column-mapping validation guards and supported use cases. Each
   * section runs against a fresh table so the test cases do not interfere with each other. Tests
   * both supported scenarios (rename, drop, add, reorder) and rejection cases.
   */
  @Test
  public void testRenameColumnGuards() throws Exception {

    // -------- Support: first-enable WITHOUT rename --------
    // Non-CM external table; send set-columns that adds CM metadata but keeps the same logical
    // names (physicalName == old logical name by convention). Should succeed with no rename error.
    {
      Handle h = createDeltaExternal("tbl_guards_first_enable_ok");
      DeltaStructFieldMetadata idMeta = new DeltaStructFieldMetadata();
      idMeta.put("delta.columnMapping.id", 1);
      idMeta.put("delta.columnMapping.physicalName", "id");
      DeltaStructFieldMetadata amtMeta = new DeltaStructFieldMetadata();
      amtMeta.put("delta.columnMapping.id", 2);
      amtMeta.put("delta.columnMapping.physicalName", "amount");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("id")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(idMeta),
                                  new DeltaStructField()
                                      .name("amount")
                                      .type(new DeltaPrimitiveType().type("double"))
                                      .nullable(true)
                                      .metadata(amtMeta)))));
      assertThat(columnNames(r)).containsExactly("id", "amount");
    }

    // -------- Support: add column (CM table) --------
    {
      Handle h = createCmNameModeManaged("tbl_guards_add_col");
      DeltaStructFieldMetadata aMeta = new DeltaStructFieldMetadata();
      aMeta.put("delta.columnMapping.id", 1);
      aMeta.put("delta.columnMapping.physicalName", "col-1");
      DeltaStructFieldMetadata newMeta = new DeltaStructFieldMetadata();
      newMeta.put("delta.columnMapping.id", 3);
      newMeta.put("delta.columnMapping.physicalName", "col-3");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("a")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(aMeta),
                                  new DeltaStructField()
                                      .name("new_col")
                                      .type(new DeltaPrimitiveType().type("string"))
                                      .nullable(true)
                                      .metadata(newMeta)))));
      assertThat(columnNames(r)).containsExactly("a", "new_col");
    }

    // -------- Support: drop non-partition column (CM table with 2 columns) --------
    // Create a CM table with 2 columns, then send set-columns with only 1 (drop the other).
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_drop_col");
      DeltaStructFieldMetadata aMeta = new DeltaStructFieldMetadata();
      aMeta.put("delta.columnMapping.id", 1);
      aMeta.put("delta.columnMapping.physicalName", "col-1");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("a")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(aMeta)))));
      assertThat(columnNames(r)).containsExactly("a");
    }

    // -------- Support: reorder columns (CM table with 2 columns) --------
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_reorder");
      DeltaStructFieldMetadata aMeta = new DeltaStructFieldMetadata();
      aMeta.put("delta.columnMapping.id", 1);
      aMeta.put("delta.columnMapping.physicalName", "col-1");
      DeltaStructFieldMetadata bMeta = new DeltaStructFieldMetadata();
      bMeta.put("delta.columnMapping.id", 2);
      bMeta.put("delta.columnMapping.physicalName", "col-2");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("string"))
                                      .nullable(true)
                                      .metadata(bMeta),
                                  new DeltaStructField()
                                      .name("a")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(aMeta)))));
      assertThat(columnNames(r)).containsExactly("b", "a");
    }

    // -------- Support: case-only change on non-CM table --------
    // namesRemovedWithoutMapping uses equalsIgnoreCase so "id" and "ID" are the same logical name.
    {
      Handle h = createDeltaExternal("tbl_guards_case_only");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("ID")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(new DeltaStructFieldMetadata()),
                                  new DeltaStructField()
                                      .name("amount")
                                      .type(new DeltaPrimitiveType().type("double"))
                                      .nullable(true)
                                      .metadata(new DeltaStructFieldMetadata())))));
      assertThat(columnNames(r)).containsExactly("ID", "amount");
    }

    // -------- Support: type change on CM column --------
    {
      Handle h = createCmNameModeManaged("tbl_guards_type_change");
      DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
      meta.put("delta.columnMapping.id", 1);
      meta.put("delta.columnMapping.physicalName", "col-1");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("a")
                                      .type(new DeltaPrimitiveType().type("double"))
                                      .nullable(false)
                                      .metadata(meta)))));
      assertThat(columnNames(r)).containsExactly("a");
      assertThat(r.getMetadata().getColumns().getFields().get(0).getType().getType())
          .isEqualTo("double");
    }

    // -------- Support: mixed 1-rename + add + drop in one request (name-mode) --------
    // Two-column CM table: rename "a"→"c" (same physicalName "col-1"), drop "b", add "d".
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_mixed_ops");
      DeltaStructFieldMetadata cMeta = new DeltaStructFieldMetadata();
      cMeta.put("delta.columnMapping.id", 1);
      cMeta.put(
          "delta.columnMapping.physicalName", "col-1"); // old column "a" retains this physicalName
      DeltaStructFieldMetadata dMeta = new DeltaStructFieldMetadata();
      dMeta.put("delta.columnMapping.id", 5);
      dMeta.put("delta.columnMapping.physicalName", "col-5");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("c")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(cMeta),
                                  new DeltaStructField()
                                      .name("d")
                                      .type(new DeltaPrimitiveType().type("string"))
                                      .nullable(true)
                                      .metadata(dMeta)))));
      assertThat(columnNames(r)).containsExactly("c", "d");
    }

    // -------- Support: rename + type change in one request (name-mode) --------
    {
      Handle h = createCmNameModeManaged("tbl_guards_rename_type");
      DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
      meta.put("delta.columnMapping.id", 1);
      meta.put("delta.columnMapping.physicalName", "col-1");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("double"))
                                      .nullable(false)
                                      .metadata(meta)))));
      assertThat(columnNames(r)).containsExactly("b");
      assertThat(r.getMetadata().getColumns().getFields().get(0).getType().getType())
          .isEqualTo("double");
    }

    // -------- Support: partition-column rename with explicit set-partition-columns --------
    // Rename "a"→"b" (same physicalName) AND send set-partition-columns(["b"]); should succeed.
    {
      Handle h = createCmNameModeManaged("tbl_guards_part_rename_explicit");
      // First make "a" a partition column.
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
                  .getMetadata()
                  .getEtag());
      // Now rename "a" → "b" and update partition list in the same request.
      DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
      meta.put("delta.columnMapping.id", 1);
      meta.put("delta.columnMapping.physicalName", "col-1");
      DeltaLoadTableResponse r =
          updateTable(
              h1,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(meta)))),
              new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")));
      assertThat(columnNames(r)).containsExactly("b");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("b");
    }

    // -------- Support: partition-column rename resolved by physicalName (no explicit
    // set-partition-columns) --------
    // Partition on "a" (physicalName "col-1"); rename "a"→"b" (same physicalName);
    // set-partition-columns is ABSENT. The mapper resolves "a" to "b" by physicalName match.
    {
      Handle h = createCmNameModeManaged("tbl_guards_part_rename_implicit_name_mode");
      // First make "a" a partition column.
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
                  .getMetadata()
                  .getEtag());
      // Now rename "a" → "b" WITHOUT sending set-partition-columns.
      DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
      meta.put("delta.columnMapping.id", 1);
      meta.put("delta.columnMapping.physicalName", "col-1"); // same physicalName
      DeltaLoadTableResponse r =
          updateTable(
              h1,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(meta)))));
      assertThat(columnNames(r)).containsExactly("b");
      // The partition should have followed the rename to "b".
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("b");
    }

    // -------- Support: partition-column rename resolved by id (id-mode, no explicit
    // set-partition-columns) --------
    // Partition on "a" (id=1); rename "a"→"b" (same id=1); set-partition-columns is ABSENT.
    // The mapper resolves "a" to "b" by id match.
    {
      Handle h = createCmIdModeManaged("tbl_guards_part_rename_implicit_id_mode");
      // First make "a" a partition column.
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
                  .getMetadata()
                  .getEtag());
      // Now rename "a" → "b" WITHOUT sending set-partition-columns.
      DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
      meta.put("delta.columnMapping.id", 1); // same id
      meta.put("delta.columnMapping.physicalName", "col-1");
      DeltaLoadTableResponse r =
          updateTable(
              h1,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(meta)))));
      assertThat(columnNames(r)).containsExactly("b");
      // The partition should have followed the rename to "b".
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("b");
    }

    // -------- Reject: partition-column dropped (no physical identity match) --------
    // Partition on "a" (id=1, physicalName "col-1"); drop "a"; send set-columns with different
    // column. No set-partition-columns sent. Mapper resolves "a" by id/physicalName: cannot find
    // it in new schema → reject with "partition column dropped".
    {
      Handle h = createCmNameModeManaged("tbl_guards_part_drop");
      // First make "a" a partition column.
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
                  .getMetadata()
                  .getEtag());
      // Now try to drop "a" by sending a new column without "a".
      DeltaStructFieldMetadata newMeta = new DeltaStructFieldMetadata();
      newMeta.put("delta.columnMapping.id", 2); // different id
      newMeta.put("delta.columnMapping.physicalName", "col-2");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h1,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("c")
                                          .type(new DeltaPrimitiveType().type("string"))
                                          .nullable(true)
                                          .metadata(newMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "partition column");
    }

    // -------- Reject: rename/drop without column mapping --------
    // External (non-CM) table; send set-columns that removes "amount" without CM metadata.
    {
      Handle h = createDeltaExternal("tbl_guards_g1");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("id")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(new DeltaStructFieldMetadata()))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Column mapping is required to rename or drop a column");
    }

    // -------- Reject: drop active mode key (id-mode table keeps physicalName, drops id) --------
    // id-mode table; incoming schema keeps physicalName but drops delta.columnMapping.id.
    {
      Handle h = createCmIdModeManaged("tbl_guards_g2_id_mode");
      DeltaStructFieldMetadata physOnly = new DeltaStructFieldMetadata();
      physOnly.put("delta.columnMapping.physicalName", "col-1"); // id dropped!
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(physOnly))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "dropped column mapping metadata");
    }

    // -----
    // name-mode table; incoming schema keeps id but drops delta.columnMapping.physicalName.
    {
      Handle h = createCmNameModeManaged("tbl_guards_g2_name_mode");
      DeltaStructFieldMetadata idOnly = new DeltaStructFieldMetadata();
      idOnly.put("delta.columnMapping.id", 1); // physicalName dropped!
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(idOnly))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "dropped column mapping metadata");
    }

    // -------- Reject: drop ALL column mapping metadata --------
    // CM table; send set-columns with a column that has NO CM metadata at all.
    {
      Handle h = createCmNameModeManaged("tbl_guards_g2");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(new DeltaStructFieldMetadata()))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "dropped column mapping metadata");
    }

    // -------- Reject: first-enable missing physicalName --------
    // Non-CM table; incoming enables CM (has id) but at least one column omits physicalName.
    // Each column gets a distinct id to avoid triggering the duplicate-id guard first.
    {
      Handle h = createDeltaExternal("tbl_guards_g3");
      DeltaStructFieldMetadata idMeta = new DeltaStructFieldMetadata();
      idMeta.put("delta.columnMapping.id", 1); // no physicalName
      DeltaStructFieldMetadata amtMeta = new DeltaStructFieldMetadata();
      amtMeta.put("delta.columnMapping.id", 2); // no physicalName
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("id")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(idMeta),
                                      new DeltaStructField()
                                          .name("amount")
                                          .type(new DeltaPrimitiveType().type("double"))
                                          .nullable(true)
                                          .metadata(amtMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "physical column name");
    }

    // -------- Reject: first-enable + rename in one request --------
    // Non-CM table; incoming enables CM AND renames "amount" → "revenue" in the same request.
    {
      Handle h = createDeltaExternal("tbl_guards_g4");
      DeltaStructFieldMetadata idMeta = new DeltaStructFieldMetadata();
      idMeta.put("delta.columnMapping.id", 1);
      idMeta.put(
          "delta.columnMapping.physicalName", "id"); // phys matches old logical "id": no rename
      DeltaStructFieldMetadata amtMeta = new DeltaStructFieldMetadata();
      amtMeta.put("delta.columnMapping.id", 2);
      amtMeta.put(
          "delta.columnMapping.physicalName",
          "amount"); // phys="amount" but logical="revenue" → rename
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("id")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(idMeta),
                                      new DeltaStructField()
                                          .name("revenue")
                                          .type(new DeltaPrimitiveType().type("double"))
                                          .nullable(true)
                                          .metadata(amtMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "separate statement");
    }

    // -------- Reject: more than one top-level rename in one request (name-mode) --------
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_g6");
      DeltaStructFieldMetadata cMeta = new DeltaStructFieldMetadata();
      cMeta.put("delta.columnMapping.id", 1);
      cMeta.put("delta.columnMapping.physicalName", "col-1"); // was "a"
      DeltaStructFieldMetadata dMeta = new DeltaStructFieldMetadata();
      dMeta.put("delta.columnMapping.id", 2);
      dMeta.put("delta.columnMapping.physicalName", "col-2"); // was "b"
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("c")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(cMeta),
                                      new DeltaStructField()
                                          .name("d")
                                          .type(new DeltaPrimitiveType().type("string"))
                                          .nullable(true)
                                          .metadata(dMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "single top-level column rename");
    }

    // -------- Reject: duplicate CM id in incoming schema (id-mode) --------
    {
      Handle h = createCmIdModeManaged("tbl_guards_g8");
      DeltaStructFieldMetadata dupMeta = new DeltaStructFieldMetadata();
      dupMeta.put("delta.columnMapping.id", 1); // SAME id as "a" → duplicate
      dupMeta.put("delta.columnMapping.physicalName", "col-99");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(dupMeta), // id=1, same as below
                                      new DeltaStructField()
                                          .name("b")
                                          .type(new DeltaPrimitiveType().type("string"))
                                          .nullable(true)
                                          .metadata(dupMeta))))), // id=1 again → duplicate
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Duplicate column mapping id");
    }

    // -------- Reject: mode-specific CM key stripped (name-mode drops physicalName, keeps id)
    // --------
    // name-mode table (active key = physicalName); two-column table: send "a" with physicalName
    // intact but "b" with physicalName stripped (only id kept).
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_g5_name_mode");
      DeltaStructFieldMetadata aOk = new DeltaStructFieldMetadata();
      aOk.put("delta.columnMapping.id", 1);
      aOk.put("delta.columnMapping.physicalName", "col-1"); // intact
      DeltaStructFieldMetadata bStripped = new DeltaStructFieldMetadata();
      bStripped.put("delta.columnMapping.id", 2); // physicalName stripped, only id kept
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(aOk),
                                      new DeltaStructField()
                                          .name("b")
                                          .type(new DeltaPrimitiveType().type("string"))
                                          .nullable(true)
                                          .metadata(bStripped))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-columns dropped column mapping metadata for column(s): b");
    }

    // -------- Reject: first-enable + NESTED rename in one request --------
    // Current table: top-level "s" is a struct column containing nested field "x" (no CM at all).
    // Incoming enables CM on "s" (physicalName "s" == current name: no outer rename) and
    // introduces nested "y" with physicalName "x" (matches current nested "x"): nested rename.
    // First-enable + nested rename in a single request is not supported.
    {
      Handle h = createSimpleStructExternal("tbl_guards_g4_nested");
      // Outer column "s": physicalName "s" (no rename, matches current "s").
      DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
      outerMeta.put("delta.columnMapping.id", 1);
      outerMeta.put("delta.columnMapping.physicalName", "s"); // phys == current name: no rename
      // Inner field "y": physicalName "x" (matches current nested name "x") but logical "y" ≠ "x".
      DeltaStructFieldMetadata innerMeta = new DeltaStructFieldMetadata();
      innerMeta.put("delta.columnMapping.id", 2);
      innerMeta.put("delta.columnMapping.physicalName", "x"); // phys matches old name → rename
      DeltaStructType newInnerType =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("y") // renamed from "x"
                          .type(new DeltaPrimitiveType().type("long"))
                          .nullable(true)
                          .metadata(innerMeta)));
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("s")
                                          .type(newInnerType)
                                          .nullable(false)
                                          .metadata(outerMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "separate statement");
    }

    // -------- Invalid mode string → rejected --------
    // Calling set-properties to set delta.columnMapping.mode="foo" is not directly testable here
    // (set-properties runs after the mode check). Instead we verify the rejection message.
    // This tests that normalizeColumnMappingMode fires on the currently-stored value by creating a
    // helper call that exercises the code path — done via a direct property mutation test below.
    // NOTE: for the round-trip test, we seed the mode via a forced property write in a separate
    // test so as not to need internal access here. The mode validation is exercised by the unit
    // test below.

    // -------- id-mode structural-scope negative control --------
    // id-mode struct table {x: long(id1), s: struct<x: string(id3)>(id2)}: unchanged set-columns
    // sends the exact same schema back. The reassignment guard must NOT flag anything (same ids,
    // no value change) even though "x" appears as both a top-level name and a nested field name.
    {
      Handle h = createIdModeComplexManaged("tbl_guards_id_scope_negative");
      // Build exact replica of the schema: top-level "x" (id=1) and "s" (id=2, struct<x(id=3)>).
      DeltaStructFieldMetadata innerXMeta = new DeltaStructFieldMetadata();
      innerXMeta.put("delta.columnMapping.id", 3);
      innerXMeta.put("delta.columnMapping.physicalName", "col-ix");
      DeltaStructType innerType =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("x")
                          .type(new DeltaPrimitiveType().type("string"))
                          .nullable(true)
                          .metadata(innerXMeta)));
      DeltaStructFieldMetadata sMeta = new DeltaStructFieldMetadata();
      sMeta.put("delta.columnMapping.id", 2);
      sMeta.put("delta.columnMapping.physicalName", "col-s");
      DeltaStructFieldMetadata xMeta = new DeltaStructFieldMetadata();
      xMeta.put("delta.columnMapping.id", 1);
      xMeta.put("delta.columnMapping.physicalName", "col-x");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("x")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(xMeta),
                                  new DeltaStructField()
                                      .name("s")
                                      .type(innerType)
                                      .nullable(false)
                                      .metadata(sMeta)))));
      // Should succeed: no id changed.
      assertThat(columnNames(r)).containsExactly("x", "s");
    }

    // -------- Reassignment: id-mode top-level --------
    // id-mode table; retained column "a" changes its CM id from 1 → 2.
    {
      Handle h = createCmIdModeManaged("tbl_guards_reassign_id");
      DeltaStructFieldMetadata changedId = new DeltaStructFieldMetadata();
      changedId.put("delta.columnMapping.id", 2); // was 1 → reassignment
      changedId.put("delta.columnMapping.physicalName", "col-1");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(changedId))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "reassignment");
    }

    // -------- Reassignment: name-mode top-level --------
    // name-mode table; retained column "a" changes its physicalName from "col-1" → "col-99".
    {
      Handle h = createCmNameModeManaged("tbl_guards_reassign_name");
      DeltaStructFieldMetadata changedPhys = new DeltaStructFieldMetadata();
      changedPhys.put("delta.columnMapping.id", 1);
      changedPhys.put("delta.columnMapping.physicalName", "col-99"); // was "col-1" → reassignment
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(changedPhys))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "reassignment");
    }

    // -------- Reassignment: nested struct field (name-mode) --------
    // name-mode struct table; the nested field "x" changes physicalName "col-x" → "col-z".
    {
      Handle h = createCmNameModeStructManaged("tbl_guards_reassign_nested_name");
      DeltaStructFieldMetadata changedNestedMeta = new DeltaStructFieldMetadata();
      changedNestedMeta.put("delta.columnMapping.id", 2);
      changedNestedMeta.put("delta.columnMapping.physicalName", "col-z"); // was "col-x"
      DeltaStructType newInnerType =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("x") // same name, physicalName changed
                          .type(new DeltaPrimitiveType().type("long"))
                          .nullable(true)
                          .metadata(changedNestedMeta)));
      DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
      outerMeta.put("delta.columnMapping.id", 1);
      outerMeta.put("delta.columnMapping.physicalName", "col-s");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("s")
                                          .type(newInnerType)
                                          .nullable(false)
                                          .metadata(outerMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "reassignment");
    }

    // -------- Reassignment: nested struct field (id-mode) -- global id scope --------
    // id-mode struct table; a nested field changes its CM id (global scope: all levels checked).
    {
      Handle h = createCmIdModeStructManaged("tbl_guards_reassign_nested_id");
      // Change nested "x"'s id from 2 to 99 (keeping physicalName "col-x" unchanged).
      DeltaStructFieldMetadata changedNestedIdMeta = new DeltaStructFieldMetadata();
      changedNestedIdMeta.put("delta.columnMapping.id", 99); // was 2
      changedNestedIdMeta.put("delta.columnMapping.physicalName", "col-x");
      DeltaStructType newInnerType =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("x")
                          .type(new DeltaPrimitiveType().type("long"))
                          .nullable(true)
                          .metadata(changedNestedIdMeta)));
      DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
      outerMeta.put("delta.columnMapping.id", 1);
      outerMeta.put("delta.columnMapping.physicalName", "col-s");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("s")
                                          .type(newInnerType)
                                          .nullable(false)
                                          .metadata(outerMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "reassignment");
    }

    // -------- Negative control: same physicalName across different struct levels is allowed
    // --------
    // Two top-level struct columns, each with a nested field using physicalName "col-inner".
    // physicalName uniqueness is only required among SIBLINGS at the same level; the same
    // physicalName appearing in two DIFFERENT nested structs is fine.
    {
      Handle h = createTwoStructColCmNameModeManaged("tbl_guards_phys_diff_levels");
      // Keep both struct columns unchanged (just re-send the same schema).
      DeltaStructFieldMetadata inner1Meta = new DeltaStructFieldMetadata();
      inner1Meta.put("delta.columnMapping.id", 3);
      inner1Meta.put("delta.columnMapping.physicalName", "col-inner"); // same as inner2
      DeltaStructFieldMetadata inner2Meta = new DeltaStructFieldMetadata();
      inner2Meta.put("delta.columnMapping.id", 4);
      inner2Meta.put(
          "delta.columnMapping.physicalName", "col-inner"); // same physName, different level
      DeltaStructType innerType1 =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("x")
                          .type(new DeltaPrimitiveType().type("long"))
                          .nullable(true)
                          .metadata(inner1Meta)));
      DeltaStructType innerType2 =
          new DeltaStructType()
              .type("struct")
              .fields(
                  List.of(
                      new DeltaStructField()
                          .name("x")
                          .type(new DeltaPrimitiveType().type("long"))
                          .nullable(true)
                          .metadata(inner2Meta)));
      DeltaStructFieldMetadata s1Meta = new DeltaStructFieldMetadata();
      s1Meta.put("delta.columnMapping.id", 1);
      s1Meta.put("delta.columnMapping.physicalName", "col-s1");
      DeltaStructFieldMetadata s2Meta = new DeltaStructFieldMetadata();
      s2Meta.put("delta.columnMapping.id", 2);
      s2Meta.put("delta.columnMapping.physicalName", "col-s2");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("s1")
                                      .type(innerType1)
                                      .nullable(false)
                                      .metadata(s1Meta),
                                  new DeltaStructField()
                                      .name("s2")
                                      .type(innerType2)
                                      .nullable(false)
                                      .metadata(s2Meta)))));
      // Should succeed: same physicalName "col-inner" at different levels is allowed.
      assertThat(columnNames(r)).containsExactly("s1", "s2");
    }

    // -------- Support: rename a column and re-add its old name in one request (name-mode) --------
    // Rename "a"→"x" (physicalName col-1 kept) and add a NEW column named "a" with a fresh
    // physicalName. The re-added "a" must not be misread as a reassignment of the renamed-away "a".
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_rename_readd");
      DeltaStructFieldMetadata xMeta = new DeltaStructFieldMetadata();
      xMeta.put("delta.columnMapping.id", 1);
      xMeta.put("delta.columnMapping.physicalName", "col-1");
      DeltaStructFieldMetadata bMeta = new DeltaStructFieldMetadata();
      bMeta.put("delta.columnMapping.id", 2);
      bMeta.put("delta.columnMapping.physicalName", "col-2");
      DeltaStructFieldMetadata newAMeta = new DeltaStructFieldMetadata();
      newAMeta.put("delta.columnMapping.id", 9);
      newAMeta.put("delta.columnMapping.physicalName", "col-9");
      DeltaLoadTableResponse r =
          updateTable(
              h,
              new DeltaSetSchemaUpdate()
                  .columns(
                      new DeltaStructType()
                          .type("struct")
                          .fields(
                              List.of(
                                  new DeltaStructField()
                                      .name("x")
                                      .type(new DeltaPrimitiveType().type("long"))
                                      .nullable(false)
                                      .metadata(xMeta),
                                  new DeltaStructField()
                                      .name("b")
                                      .type(new DeltaPrimitiveType().type("string"))
                                      .nullable(true)
                                      .metadata(bMeta),
                                  new DeltaStructField()
                                      .name("a")
                                      .type(new DeltaPrimitiveType().type("string"))
                                      .nullable(true)
                                      .metadata(newAMeta)))));
      assertThat(columnNames(r)).containsExactly("x", "b", "a");
    }

    // -------- Behavior: a rename regenerates the DB row UUID while preserving CM identity --------
    {
      Handle h = createCmNameModeManaged("tbl_guards_uuid_regen");
      UUID before = columnIdByName(h, "a");
      DeltaLoadTableResponse r = updateTable(h, setColumnsRenaming("a", "b", "col-1"));
      assertThat(columnNames(r)).containsExactly("b");
      // Row UUID is regenerated on set-columns; nothing keys on it, and the column-mapping identity
      // (physicalName) is what carries the column's identity across the rename.
      assertThat(columnIdByName(h, "b")).isNotEqualTo(before);
      assertThat(
              r.getMetadata()
                  .getColumns()
                  .getFields()
                  .get(0)
                  .getMetadata()
                  .get("delta.columnMapping.physicalName"))
          .isEqualTo("col-1");
    }

    // -------- Reject: duplicate top-level physicalName (name-mode) --------
    {
      Handle h = createTwoColCmNameModeManaged("tbl_guards_dup_phys");
      DeltaStructFieldMetadata aMeta = new DeltaStructFieldMetadata();
      aMeta.put("delta.columnMapping.id", 1);
      aMeta.put("delta.columnMapping.physicalName", "col-dup");
      DeltaStructFieldMetadata bMeta = new DeltaStructFieldMetadata();
      bMeta.put("delta.columnMapping.id", 2);
      bMeta.put("delta.columnMapping.physicalName", "col-dup");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("a")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(aMeta),
                                      new DeltaStructField()
                                          .name("b")
                                          .type(new DeltaPrimitiveType().type("string"))
                                          .nullable(true)
                                          .metadata(bMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Duplicate column mapping physicalName 'col-dup'");
    }

    // -------- Reject: first-enable with an empty physicalName --------
    {
      Handle h = createDeltaExternal("tbl_guards_empty_phys");
      DeltaStructFieldMetadata idMeta = new DeltaStructFieldMetadata();
      idMeta.put("delta.columnMapping.id", 1);
      idMeta.put("delta.columnMapping.physicalName", "id");
      DeltaStructFieldMetadata amtMeta = new DeltaStructFieldMetadata();
      amtMeta.put("delta.columnMapping.id", 2);
      amtMeta.put("delta.columnMapping.physicalName", "");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("id")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(idMeta),
                                      new DeltaStructField()
                                          .name("amount")
                                          .type(new DeltaPrimitiveType().type("double"))
                                          .nullable(true)
                                          .metadata(amtMeta))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "requires a physical column name");
    }

    // -------- Reject: struct column changed to a scalar without column mapping --------
    // Non-CM external table with s: struct<x>. Changing "s" to a scalar drops nested field "x";
    // without column mapping that is a destructive rewrite and must be rejected.
    {
      Handle h = createSimpleStructExternal("tbl_guards_struct_to_scalar");
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new DeltaSetSchemaUpdate()
                      .columns(
                          new DeltaStructType()
                              .type("struct")
                              .fields(
                                  List.of(
                                      new DeltaStructField()
                                          .name("s")
                                          .type(new DeltaPrimitiveType().type("long"))
                                          .nullable(false)
                                          .metadata(new DeltaStructFieldMetadata()))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Column mapping is required to rename or drop a column");
    }
  }

  // ---------------------------------------------------------------- helpers

  /** Pins the update to the table's UUID and the Handle's etag. */
  private DeltaLoadTableResponse updateTable(Handle h, DeltaTableUpdate... updates)
      throws ApiException {
    return updateTable(h.name(), requestWith(h.tableId(), Optional.of(h.etag()), updates));
  }

  /** Escape hatch for tests that hand-build the request (custom requirements / empty updates). */
  private DeltaLoadTableResponse updateTable(String tableName, DeltaUpdateTableRequest request)
      throws ApiException {
    return deltaTablesApi.updateTable(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName, request);
  }

  /**
   * Build an {@link DeltaUpdateTableRequest} with the canonical {@code assert-table-uuid}
   * requirement and an optional {@code assert-etag} requirement. Empty {@code updates} is allowed
   * for the empty-list rejection case.
   */
  private static DeltaUpdateTableRequest requestWith(
      UUID assertUuid, Optional<String> assertEtag, DeltaTableUpdate... updates) {
    List<DeltaTableRequirement> requirements = new ArrayList<>();
    requirements.add(new DeltaAssertTableUUID().uuid(assertUuid));
    assertEtag.ifPresent(etag -> requirements.add(new DeltaAssertEtag().etag(etag)));
    return new DeltaUpdateTableRequest().requirements(requirements).updates(List.of(updates));
  }

  /** Convenience: assert-table-uuid only, no assert-etag. */
  private static DeltaUpdateTableRequest requestWith(UUID assertUuid, DeltaTableUpdate... updates) {
    return requestWith(assertUuid, Optional.empty(), updates);
  }

  // -------------------------------------------------------- CM table creation helpers

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: single column "a"
   * (long, not null, physicalName "col-1", delta.columnMapping.id 1).
   */
  private Handle createCmNameModeManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "1");
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    meta.put("delta.columnMapping.id", 1);
    meta.put("delta.columnMapping.physicalName", "col-1");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("a")
                                    .type(new DeltaPrimitiveType().type("long"))
                                    .nullable(false)
                                    .metadata(meta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: single top-level
   * struct column "s" (physicalName "col-s", id 1) containing nested field "x" (physicalName
   * "col-x", id 2). Used by the nested-rename tests.
   */
  private Handle createCmNameModeStructManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "2");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-x");
    DeltaStructType innerType =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-s");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("s")
                                    .type(innerType)
                                    .nullable(false)
                                    .metadata(outerMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED Delta table with column-mapping id-mode enabled. Schema: single column "a"
   * (long, not null, delta.columnMapping.id 1, physicalName "col-1").
   */
  private Handle createCmIdModeManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "id");
    properties.put("delta.columnMapping.maxColumnId", "1");
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    meta.put("delta.columnMapping.id", 1);
    meta.put("delta.columnMapping.physicalName", "col-1");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("a")
                                    .type(new DeltaPrimitiveType().type("long"))
                                    .nullable(false)
                                    .metadata(meta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: two columns "a"
   * (long, not null, id 1, physicalName "col-1") and "b" (string, nullable, id 2, physicalName
   * "col-2"). Used by tests that need two columns (drop, reorder, multi-rename).
   */
  private Handle createTwoColCmNameModeManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "2");
    DeltaStructFieldMetadata aMeta = new DeltaStructFieldMetadata();
    aMeta.put("delta.columnMapping.id", 1);
    aMeta.put("delta.columnMapping.physicalName", "col-1");
    DeltaStructFieldMetadata bMeta = new DeltaStructFieldMetadata();
    bMeta.put("delta.columnMapping.id", 2);
    bMeta.put("delta.columnMapping.physicalName", "col-2");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("a")
                                    .type(new DeltaPrimitiveType().type("long"))
                                    .nullable(false)
                                    .metadata(aMeta),
                                new DeltaStructField()
                                    .name("b")
                                    .type(new DeltaPrimitiveType().type("string"))
                                    .nullable(true)
                                    .metadata(bMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED name-mode Delta table with a single {@code map<string, struct<val>>} column
   * "props" (id 1, physicalName "col-props"); the value struct's field "val" has id 2, physicalName
   * "col-val". Used by the map-value nested rename test.
   */
  private Handle createCmNameModeMapStructManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "2");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-val");
    DeltaStructType valueStruct =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("val")
                        .type(new DeltaPrimitiveType().type("string"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-props");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("props")
                                    .type(
                                        new DeltaMapType()
                                            .type("map")
                                            .keyType(new DeltaPrimitiveType().type("string"))
                                            .valueType(valueStruct)
                                            .valueContainsNull(true))
                                    .nullable(true)
                                    .metadata(outerMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED name-mode Delta table with a single {@code array<struct<label>>} column
   * "events" (id 1, physicalName "col-events"); the element struct's field "label" has id 2,
   * physicalName "col-label". Used by the array-element nested rename test.
   */
  private Handle createCmNameModeArrayStructManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "2");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-label");
    DeltaStructType elementStruct =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("label")
                        .type(new DeltaPrimitiveType().type("string"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-events");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("events")
                                    .type(
                                        new DeltaArrayType()
                                            .type("array")
                                            .elementType(elementStruct)
                                            .containsNull(true))
                                    .nullable(true)
                                    .metadata(outerMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED Delta table with column-mapping id-mode enabled. Schema: single top-level
   * struct column "s" (id 1, physicalName "col-s") containing nested field "x" (id 2, physicalName
   * "col-x"). Used by the nested id-mode reassignment guard test.
   */
  private Handle createCmIdModeStructManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "id");
    properties.put("delta.columnMapping.maxColumnId", "2");
    DeltaStructFieldMetadata nestedMeta = new DeltaStructFieldMetadata();
    nestedMeta.put("delta.columnMapping.id", 2);
    nestedMeta.put("delta.columnMapping.physicalName", "col-x");
    DeltaStructType innerType =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(nestedMeta)));
    DeltaStructFieldMetadata outerMeta = new DeltaStructFieldMetadata();
    outerMeta.put("delta.columnMapping.id", 1);
    outerMeta.put("delta.columnMapping.physicalName", "col-s");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("s")
                                    .type(innerType)
                                    .nullable(false)
                                    .metadata(outerMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: two top-level
   * struct columns "s1" and "s2", each containing a nested field "x" with physicalName "col-inner".
   * Used to verify that the same physicalName reused across different nested-struct levels is not
   * flagged as a reassignment (physicalName uniqueness is same-level only).
   */
  private Handle createTwoStructColCmNameModeManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "name");
    properties.put("delta.columnMapping.maxColumnId", "4");

    DeltaStructFieldMetadata inner1Meta = new DeltaStructFieldMetadata();
    inner1Meta.put("delta.columnMapping.id", 3);
    inner1Meta.put("delta.columnMapping.physicalName", "col-inner"); // same as inner2
    DeltaStructType innerType1 =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(inner1Meta)));
    DeltaStructFieldMetadata s1Meta = new DeltaStructFieldMetadata();
    s1Meta.put("delta.columnMapping.id", 1);
    s1Meta.put("delta.columnMapping.physicalName", "col-s1");

    DeltaStructFieldMetadata inner2Meta = new DeltaStructFieldMetadata();
    inner2Meta.put("delta.columnMapping.id", 4);
    inner2Meta.put("delta.columnMapping.physicalName", "col-inner"); // same physName, diff level
    DeltaStructType innerType2 =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(inner2Meta)));
    DeltaStructFieldMetadata s2Meta = new DeltaStructFieldMetadata();
    s2Meta.put("delta.columnMapping.id", 2);
    s2Meta.put("delta.columnMapping.physicalName", "col-s2");

    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("s1")
                                    .type(innerType1)
                                    .nullable(false)
                                    .metadata(s1Meta),
                                new DeltaStructField()
                                    .name("s2")
                                    .type(innerType2)
                                    .nullable(false)
                                    .metadata(s2Meta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /**
   * Creates an EXTERNAL Delta table with a single top-level struct column "s" containing a nested
   * field "x" (long, no CM metadata on either field). Used to test nested rename detection on the
   * first-enable path.
   */
  private Handle createSimpleStructExternal(String tableName) throws Exception {
    String location =
        java.nio.file.Files.createTempDirectory(testDirectoryRoot, "external_").toString();
    DeltaStructType innerType =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("long"))
                        .nullable(true)
                        .metadata(new DeltaStructFieldMetadata())));
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(location)
                .tableType(DeltaTableType.EXTERNAL)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("s")
                                    .type(innerType)
                                    .nullable(false)
                                    .metadata(new DeltaStructFieldMetadata()))))
                .protocol(
                    new DeltaProtocol()
                        .minReaderVersion(3)
                        .minWriterVersion(7)
                        .readerFeatures(List.of(TableFeature.DELETION_VECTORS.specName()))
                        .writerFeatures(List.of(TableFeature.DELETION_VECTORS.specName())))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(Map.of("delta.enableDeletionVectors", "true")));
    return new Handle(tableName, resp.getMetadata().getTableUuid(), resp.getMetadata().getEtag());
  }

  /**
   * Creates an id-mode MANAGED Delta table with a complex schema: top-level "x" (long, id=1,
   * physicalName "col-x") and "s" (struct<x: string(id=3)>, id=2, physicalName "col-s"). The nested
   * "x" has id=3 and physicalName "col-ix". Used to test the id-mode structural-scope negative
   * control (same logical name "x" at top level and inside struct).
   */
  private Handle createIdModeComplexManaged(String tableName) throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", "id");
    properties.put("delta.columnMapping.maxColumnId", "3");
    DeltaStructFieldMetadata innerXMeta = new DeltaStructFieldMetadata();
    innerXMeta.put("delta.columnMapping.id", 3);
    innerXMeta.put("delta.columnMapping.physicalName", "col-ix");
    DeltaStructType innerType =
        new DeltaStructType()
            .type("struct")
            .fields(
                List.of(
                    new DeltaStructField()
                        .name("x")
                        .type(new DeltaPrimitiveType().type("string"))
                        .nullable(true)
                        .metadata(innerXMeta)));
    DeltaStructFieldMetadata sMeta = new DeltaStructFieldMetadata();
    sMeta.put("delta.columnMapping.id", 2);
    sMeta.put("delta.columnMapping.physicalName", "col-s");
    DeltaStructFieldMetadata xMeta = new DeltaStructFieldMetadata();
    xMeta.put("delta.columnMapping.id", 1);
    xMeta.put("delta.columnMapping.physicalName", "col-x");
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(
                    new DeltaStructType()
                        .type("struct")
                        .fields(
                            List.of(
                                new DeltaStructField()
                                    .name("x")
                                    .type(new DeltaPrimitiveType().type("long"))
                                    .nullable(false)
                                    .metadata(xMeta),
                                new DeltaStructField()
                                    .name("s")
                                    .type(innerType)
                                    .nullable(false)
                                    .metadata(sMeta))))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  // -------------------------------------------------------- CM update helpers

  /**
   * Builds a {@code set-columns} update that renames a single column from {@code oldName} to {@code
   * newName}, preserving the given {@code physicalName} in the column-mapping metadata (name-mode:
   * keyed on physicalName; CM id remains 1).
   */
  private static DeltaSetSchemaUpdate setColumnsRenaming(
      String oldName, String newName, String physicalName) {
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    meta.put("delta.columnMapping.id", 1);
    meta.put("delta.columnMapping.physicalName", physicalName);
    return new DeltaSetSchemaUpdate()
        .columns(
            new DeltaStructType()
                .type("struct")
                .fields(
                    List.of(
                        new DeltaStructField()
                            .name(newName)
                            .type(new DeltaPrimitiveType().type("long"))
                            .nullable(false)
                            .metadata(meta))));
  }

  /**
   * Builds a {@code set-columns} update that renames a single column from {@code oldName} to {@code
   * newName} in id-mode, preserving the given {@code cmId} and {@code physicalName}.
   */
  private static DeltaSetSchemaUpdate setColumnsRenamingIdMode(
      String oldName, String newName, int cmId, String physicalName) {
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    meta.put("delta.columnMapping.id", cmId);
    meta.put("delta.columnMapping.physicalName", physicalName);
    return new DeltaSetSchemaUpdate()
        .columns(
            new DeltaStructType()
                .type("struct")
                .fields(
                    List.of(
                        new DeltaStructField()
                            .name(newName)
                            .type(new DeltaPrimitiveType().type("long"))
                            .nullable(false)
                            .metadata(meta))));
  }

  /**
   * Builds a {@code set-columns} update for column {@code colName} with its column-mapping metadata
   * stripped (empty metadata). Used by the test that verifies stripping CM keys is rejected.
   */
  private static DeltaSetSchemaUpdate setColumnsWithoutMappingMetadataFor(String colName) {
    return new DeltaSetSchemaUpdate()
        .columns(
            new DeltaStructType()
                .type("struct")
                .fields(
                    List.of(
                        new DeltaStructField()
                            .name(colName)
                            .type(new DeltaPrimitiveType().type("long"))
                            .nullable(false)
                            .metadata(new DeltaStructFieldMetadata()))));
  }

  // -------------------------------------------------------- DB-level assertion helpers

  /**
   * Returns the {@code ColumnInfoDAO.id} UUID for the column with the given logical name in the
   * table identified by {@code h}. Reads directly from the in-process Hibernate session factory so
   * the test can assert on the DB-level identity that the REST API does not expose.
   */
  private UUID columnIdByName(Handle h, String colName) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session
          .createQuery(
              "select c.id from ColumnInfoDAO c"
                  + " where c.table.id = :tableId and c.name = :name",
              UUID.class)
          .setParameter("tableId", h.tableId())
          .setParameter("name", colName)
          .getSingleResult();
    }
  }

  /** Returns the ordered list of top-level column names from a {@link DeltaLoadTableResponse}. */
  private static List<String> columnNames(DeltaLoadTableResponse resp) {
    return resp.getMetadata().getColumns().getFields().stream()
        .map(DeltaStructField::getName)
        .collect(Collectors.toList());
  }
}
