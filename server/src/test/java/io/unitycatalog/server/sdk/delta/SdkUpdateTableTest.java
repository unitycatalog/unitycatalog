package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.client.delta.model.DeltaAssertEtag;
import io.unitycatalog.client.delta.model.DeltaAssertTableUUID;
import io.unitycatalog.client.delta.model.DeltaClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.DeltaDomainMetadataUpdates;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
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
import io.unitycatalog.client.delta.model.DeltaStructField;
import io.unitycatalog.client.delta.model.DeltaStructType;
import io.unitycatalog.client.delta.model.DeltaTableRequirement;
import io.unitycatalog.client.delta.model.DeltaTableUpdate;
import io.unitycatalog.client.delta.model.DeltaUniformMetadata;
import io.unitycatalog.client.delta.model.DeltaUniformMetadataIceberg;
import io.unitycatalog.client.delta.model.DeltaUpdateSnapshotVersionUpdate;
import io.unitycatalog.client.delta.model.DeltaUpdateTableRequest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
      assertThat(r1.getMetadata().getEtag()).isNotEqualTo(h.etag());

      // Phase 2: remove-domain-metadata alone -- now the rowTracking entry must disappear.
      DeltaLoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new DeltaRemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking")));
      assertThat(r2.getMetadata().getProperties())
          .doesNotContainKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK)
          .containsKey(TableProperties.CLUSTERING_COLUMNS);
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
    {
      Handle h = createDeltaExternal("tbl_setcols");
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
                                      .metadata(Map.of()),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(Map.of())))),
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
    // schema that still contains id. Partition list must survive the schema swap; a silent
    // clear would desynchronize the partition list from the column definitions.
    // (createDeltaExternal seeds (id long, amount double); only `id` exists in both old and new.)
    {
      Handle h = createDeltaExternal("tbl_setcols_preserve_part");
      DeltaLoadTableResponse partitionSetup =
          updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("id")));
      Handle h1 = h.withEtag(partitionSetup.getMetadata().getEtag());
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
                                      .metadata(Map.of()),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(Map.of())))));
      assertThat(r.getMetadata().getColumns().getFields())
          .extracting(DeltaStructField::getName)
          .containsExactly("id", "flag");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("id");
    }

    // -------- set-columns alone that drops a previously-partition column is rejected --------
    // Partition is on `id`; the new schema replaces `id` with `id2`. Silent partition clearing
    // would leave the table inconsistent, so this must fail with the unknown-column error.
    {
      Handle h = createDeltaExternal("tbl_setcols_drop_part");
      Handle h1 =
          h.withEtag(
              updateTable(h, new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("id")))
                  .getMetadata()
                  .getEtag());
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
                                          .metadata(Map.of()))))),
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

      // assert-table-uuid mismatch: rejected during checkRequirements.
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
      io.unitycatalog.client.model.TableInfo external =
          createTestingTable(
              "tbl_external_parquet",
              io.unitycatalog.client.model.TableType.EXTERNAL,
              Optional.of(testDirectoryRoot.toString()),
              io.unitycatalog.client.model.DataSourceFormat.PARQUET,
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

      // Replaying v1 should surface as a CommitVersionConflict (409). Pin assert-etag against
      // the post-r1 etag so the conflict comes from the version check, not from assert-etag.
      Handle h1 = h.withEtag(r1.getMetadata().getEtag());
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
    // Exercises the standalone backfill branch (handleBackfillOnlyCommit) that the combined
    // add-commit + backfill test doesn't reach (that one goes through handleNormalCommit). The
    // post-backfill commits list isn't asserted: markCommitAsLatestBackfilled issues a native
    // SQL UPDATE that the session cache doesn't reflect in the same-transaction read used to
    // build the response.
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
      assertThat(r2.getMetadata().getEtag()).isNotEqualTo(r1.getMetadata().getEtag());
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
    {
      Handle h = createDeltaManaged("tbl_commit_meta_stamp_schema", Map.of());
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
                                      .metadata(Map.of())))));
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
}
