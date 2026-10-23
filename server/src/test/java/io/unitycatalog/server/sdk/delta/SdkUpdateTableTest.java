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
import io.unitycatalog.client.delta.model.DeltaDataType;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
                                      .metadata(new DeltaStructFieldMetadata()),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(new DeltaStructFieldMetadata())))),
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
                                      .metadata(new DeltaStructFieldMetadata()),
                                  new DeltaStructField()
                                      .name("flag")
                                      .type(new DeltaPrimitiveType().type("boolean"))
                                      .nullable(true)
                                      .metadata(new DeltaStructFieldMetadata())))));
      assertThat(r.getMetadata().getColumns().getFields())
          .extracting(DeltaStructField::getName)
          .containsExactly("id", "flag");
      assertThat(r.getMetadata().getPartitionColumns()).containsExactly("id");
    }

    // -------- set-columns alone that drops a previously-partition column is rejected --------
    // Partition is on `id`; the new schema replaces `id` with `id2`. Silent partition clearing
    // would leave the table inconsistent, so this must fail with the dropped-partition error.
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
                                          .metadata(new DeltaStructFieldMetadata()))))),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "partition column 'id' dropped");
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
      writePublishedCommitFile(r1.getMetadata().getLocation(), 1L);
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

    // -------- add-commit + backfill over an unpublished intermediate version is rejected --------
    // The whole request fails before anything is persisted: the commit, the purge, and the other
    // updates in the request are all left undone, and the etag is unchanged.
    {
      Handle h = createDeltaManaged("tbl_commit_backfill_gap", Map.of());
      Handle cur = h;
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
      DeltaLoadTableResponse before =
          deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
      String loc = before.getMetadata().getLocation();
      // Publish v1 and v3 only; v2 is missing from the middle of the backfill range.
      writePublishedCommitFile(loc, 1L);
      writePublishedCommitFile(loc, 3L);

      DeltaTableUpdate[] commitV4AndBackfillV3 = {
        new DeltaAddCommitUpdate()
            .commit(
                new DeltaCommit()
                    .version(4L)
                    .timestamp(1700000004L)
                    .fileName("00000004.json")
                    .fileSize(1024L)
                    .fileModificationTimestamp(1700000004L)),
        new DeltaSetPropertiesUpdate().updates(Map.of("k", "v")),
        new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(3L)
      };
      TestUtils.assertDeltaApiException(
          () -> updateTable(h3, commitV4AndBackfillV3),
          DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "published commit file is missing");
      DeltaLoadTableResponse afterReject =
          deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
      assertThat(afterReject.getLatestTableVersion()).isEqualTo(3L);
      assertThat(afterReject.getCommits()).hasSize(3);
      assertThat(afterReject.getMetadata().getEtag()).isEqualTo(before.getMetadata().getEtag());
      assertThat(afterReject.getMetadata().getProperties()).doesNotContainKey("k");

      // Once v2 is published, resending the same request commits v4, applies the property, and
      // purges through v3.
      writePublishedCommitFile(loc, 2L);
      DeltaLoadTableResponse accepted = updateTable(h3, commitV4AndBackfillV3);
      assertThat(accepted.getLatestTableVersion()).isEqualTo(4L);
      assertThat(accepted.getCommits()).hasSize(1);
      assertThat(accepted.getCommits().get(0).getVersion()).isEqualTo(4L);
      assertThat(accepted.getMetadata().getProperties()).containsEntry("k", "v");
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
      writePublishedCommitFile(r1.getMetadata().getLocation(), 1L);
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
                                      .metadata(new DeltaStructFieldMetadata())))));
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
      // Publish v1 and v2 before backfill so purge verification succeeds; then purge leaves [v3].
      byte[] v1Content = "delta-commit-v1\n".getBytes(StandardCharsets.UTF_8);
      byte[] v2Content = "delta-commit-v2\n".getBytes(StandardCharsets.UTF_8);
      writeTableFile(loc, "_delta_log/00000000000000000001.json", v1Content);
      writeTableFile(loc, "_delta_log/00000000000000000002.json", v2Content);
      DeltaLoadTableResponse afterBackfill =
          updateTable(cur, new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(2L));
      final Handle h3 = cur.withEtag(afterBackfill.getMetadata().getEtag());

      // Stage a byte-identical file for v2: the replay is recognized by content and is an
      // idempotent no-op success (table still at v3).
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

  /** Write a dummy published {@code _delta_log/<version>.json} under the table location. */
  private void writePublishedCommitFile(String storageLocation, long version) throws IOException {
    writeTableFile(
        storageLocation,
        String.format("_delta_log/%020d.json", version),
        ("delta-commit-v" + version + "\n").getBytes(StandardCharsets.UTF_8));
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
   * semantics).
   */
  @Test
  public void testRenameColumnNameMode() throws Exception {
    Handle h = createCmNameModeManaged("tbl_rename_name_mode");
    DeltaLoadTableResponse r =
        updateTable(h, setColumns(fld("b", prim("long"), false, 1, "col-1")));
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
   * two-column table: "a" retains its CM key, "b" loses it, so "b" is the offending column. Also
   * covers nested struct, map-value and array-element fields (dotted path in the message), id mode,
   * and unreadable stored columns.
   */
  @Test
  public void testSetColumnsDroppingColumnMappingMetadataIsRejected() throws Exception {
    // "a" retains its CM key, "b" loses it.
    Handle h = createTwoColCmNameModeManaged("tbl_drop_cm_meta");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                h,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"),
                    fld("b", prim("string"), null, null))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): b");

    // Nested fields are checked too, and reported by their dotted path.
    Handle nested = createCmNameModeStructManaged("tbl_drop_cm_meta_nested");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                nested,
                setColumns(
                    fld("s", struct(fld("x", prim("long"), null, null)), false, 1, "col-s"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): s.x");

    // Map value structs are checked.
    Handle map = createCmNameModeMapStructManaged("tbl_drop_cm_meta_map");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                map,
                setColumns(
                    fld(
                        "props",
                        map(prim("string"), struct(fld("val", prim("string"), null, null))),
                        1,
                        "col-props"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): props.value.val");

    // Array element structs are checked.
    Handle array = createCmNameModeArrayStructManaged("tbl_drop_cm_meta_array");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                array,
                setColumns(
                    fld(
                        "events",
                        array(struct(fld("label", prim("string"), null, null))),
                        1,
                        "col-events"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): events.label");

    // Id mode checks the id key: a retained column that loses its id is rejected.
    Handle idMode = createCmIdModeManaged("tbl_drop_cm_meta_id_mode");
    TestUtils.assertDeltaApiException(
        () -> updateTable(idMode, setColumns(fld("a", prim("long"), false, null, "col-1"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): a");

    // A stored column whose type cannot be read has no identity to preserve, so a set-columns
    // that repairs it is accepted.
    Handle corrupt = createCmNameModeManaged("tbl_drop_cm_meta_corrupt");
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      session
          .createMutationQuery(
              "update ColumnInfoDAO c set c.typeJson = :json where c.table.id = :id")
          .setParameter("json", "not json")
          .setParameter("id", corrupt.tableId())
          .executeUpdate();
      tx.commit();
    }
    updateTable(corrupt, setColumns(fld("a", prim("long"), false, 1, "col-1")));

    // Stored types that parse to nothing, or to a field with null metadata, are equally
    // unreadable or empty, and do not fail the request.
    for (String json :
        List.of(
            "null", "{\"name\":\"a\",\"type\":\"long\",\"nullable\":false,\"metadata\":null}")) {
      Handle odd = createCmNameModeManaged("tbl_drop_cm_meta_odd_" + Math.abs(json.hashCode()));
      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        Transaction tx = session.beginTransaction();
        session
            .createMutationQuery(
                "update ColumnInfoDAO c set c.typeJson = :json where c.table.id = :id")
            .setParameter("json", json)
            .setParameter("id", odd.tableId())
            .executeUpdate();
        tx.commit();
      }
      updateTable(odd, setColumns(fld("a", prim("long"), false, 1, "col-1")));
    }
  }

  /**
   * Renaming a nested field "x" → "y" inside a top-level struct column "s" (outer logical name and
   * physicalName unchanged) succeeds: the outer column "s" is still present with the same outer
   * physicalName; the nested field appears as "y" inside the struct type.
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
    // The renamed child keeps its name, id and physicalName.
    DeltaStructFieldMetadata childMeta = nestedField(reloaded, "s", "y").getMetadata();
    assertThat(nestedFieldNames(reloaded, "s")).containsExactly("y");
    assertThat(childMeta.get("delta.columnMapping.id")).isEqualTo(2);
    assertThat(childMeta.get("delta.columnMapping.physicalName")).isEqualTo("col-x");
  }

  /**
   * Renaming a field inside a {@code map<string, struct<...>>} value struct ("val" → "value")
   * succeeds as a nested type_json passthrough: the top-level column "props" is retained with its
   * outer physicalName, and the nested rename rides inside the value struct.
   */
  @Test
  public void testMapStructNestedFieldRename() throws Exception {
    Handle h = createCmNameModeMapStructManaged("tbl_map_struct_rename");
    updateTable(
        h,
        setColumns(
            fld(
                "props",
                map(prim("string"), struct(fld("value", prim("string"), 2, "col-val"))),
                1,
                "col-props")));
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("props");
    assertThat(nestedField(reloaded, "props").getMetadata().get("delta.columnMapping.physicalName"))
        .isEqualTo("col-props");
    DeltaStructFieldMetadata childMeta = nestedField(reloaded, "props", "value").getMetadata();
    assertThat(childMeta.get("delta.columnMapping.id")).isEqualTo(2);
    assertThat(childMeta.get("delta.columnMapping.physicalName")).isEqualTo("col-val");
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
    updateTable(
        h,
        setColumns(
            fld(
                "events",
                array(struct(fld("tag", prim("string"), 2, "col-label"))),
                1,
                "col-events")));
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("events");
    assertThat(
            nestedField(reloaded, "events").getMetadata().get("delta.columnMapping.physicalName"))
        .isEqualTo("col-events");
    DeltaStructFieldMetadata childMeta = nestedField(reloaded, "events", "tag").getMetadata();
    assertThat(childMeta.get("delta.columnMapping.id")).isEqualTo(2);
    assertThat(childMeta.get("delta.columnMapping.physicalName")).isEqualTo("col-label");
  }

  @Test
  public void testNestedRenamesKeepColumnMappingIdentity() throws Exception {
    // struct<struct>: s.in.x -> s.in.y, every id and physical name kept.
    Handle deep =
        createManaged(
            "tbl_nested_rename_two_levels",
            "name",
            3,
            fld(
                "s",
                struct(fld("in", struct(fld("x", prim("long"), 3, "col-x")), 2, "col-in")),
                1,
                "col-s"));
    DeltaLoadTableResponse r1 =
        updateTable(
            deep,
            setColumns(
                fld(
                    "s",
                    struct(fld("in", struct(fld("y", prim("long"), 3, "col-x")), 2, "col-in")),
                    1,
                    "col-s")));
    assertThat(nestedFieldNames(r1, "s", "in")).containsExactly("y");
    DeltaStructFieldMetadata yMeta = nestedField(r1, "s", "in", "y").getMetadata();
    assertThat(yMeta.get("delta.columnMapping.id")).isEqualTo(3);
    assertThat(yMeta.get("delta.columnMapping.physicalName")).isEqualTo("col-x");

    // Two fields of one struct renamed in the same request.
    Handle two =
        createManaged(
            "tbl_nested_rename_two_fields",
            "name",
            3,
            fld(
                "s",
                struct(fld("x", prim("long"), 2, "col-x"), fld("z", prim("long"), 3, "col-z")),
                1,
                "col-s"));
    DeltaLoadTableResponse r2 =
        updateTable(
            two,
            setColumns(
                fld(
                    "s",
                    struct(
                        fld("x2", prim("long"), 2, "col-x"), fld("z2", prim("long"), 3, "col-z")),
                    1,
                    "col-s")));
    assertThat(nestedFieldNames(r2, "s")).containsExactly("x2", "z2");
    assertThat(nestedField(r2, "s", "x2").getMetadata().get("delta.columnMapping.id")).isEqualTo(2);
    assertThat(nestedField(r2, "s", "z2").getMetadata().get("delta.columnMapping.physicalName"))
        .isEqualTo("col-z");
  }

  /**
   * A byte-identical retry of an accepted set-columns + add-commit still resolves to its original
   * outcome (200) after a later commit enabled column mapping and changed the stored schema,
   * whether the retried version is an older one or the latest. The whole request is settled before
   * the set-columns guard runs, so the guard cannot turn a recognized replay into a 400. A commit
   * at a new version still gets the guard, and a different commit file at a taken version is a
   * conflict.
   */
  @Test
  public void testCommitReplayAfterColumnMappingEnabledSucceeds() throws Exception {
    Handle h = createDeltaManaged("tbl_replay_after_cm", Map.of());
    DeltaCommit v1 = commit(1L, "00000001.json");
    DeltaCommit v2 = commit(2L, "00000002.json");
    DeltaSetSchemaUpdate plain =
        setColumns(
            fld("id", prim("long"), false, null, null),
            fld("amount", prim("double"), null, null),
            fld("a", prim("long"), null, null));
    DeltaLoadTableResponse r1 = updateTable(h, plain, new DeltaAddCommitUpdate().commit(v1));
    DeltaLoadTableResponse r2 =
        updateTable(
            h.withEtag(r1.getMetadata().getEtag()),
            new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", "name")),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 2, "amount"),
                fld("a", prim("long"), 3, "a"),
                fld("b", prim("long"), 4, "col-b")),
            new DeltaAddCommitUpdate().commit(v2));
    assertThat(r2.getLatestTableVersion()).isEqualTo(2L);

    // Resend v1 (an older version) and v2 (the latest) with key-less columns: still replays.
    updateTable(h, plain, new DeltaAddCommitUpdate().commit(v1));
    updateTable(h, plain, new DeltaAddCommitUpdate().commit(v2));

    // The replays changed nothing: the stored identity and the column added by v2 remain.
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("id", "amount", "a", "b");
    assertThat(nestedField(reloaded, "a").getMetadata().get("delta.columnMapping.physicalName"))
        .isEqualTo("a");

    // A different commit file at a taken version is a conflict, not a guard rejection.
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                h, plain, new DeltaAddCommitUpdate().commit(commit(2L, "other-00000002.json"))),
        DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION,
        "Commit version already accepted");

    // A commit at a new version is not a replay, so the guard still applies.
    TestUtils.assertDeltaApiException(
        () -> updateTable(h, plain, new DeltaAddCommitUpdate().commit(commit(3L, "00000003.json"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): a, amount, id");
  }

  private static DeltaCommit commit(long version, String fileName) {
    return new DeltaCommit()
        .version(version)
        .timestamp(1700000000L + version)
        .fileName(fileName)
        .fileSize(1024L)
        .fileModificationTimestamp(1700000000L + version);
  }

  /**
   * Top-level rename on an id-mode table (CM identity keyed on {@code delta.columnMapping.id}).
   * After "a" → "b" the new logical name is present, old is gone, and the CM id remains 1 in the
   * column's type_json.
   */
  @Test
  public void testRenameColumnIdMode() throws Exception {
    Handle h = createCmIdModeManaged("tbl_rename_id_mode");
    DeltaLoadTableResponse r =
        updateTable(h, setColumns(fld("b", prim("long"), false, 1, "col-1")));
    assertThat(columnNames(r)).containsExactly("b");
    DeltaLoadTableResponse reloaded =
        deltaTablesApi.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name());
    assertThat(columnNames(reloaded)).containsExactly("b");
    // CM id must survive the rename in type_json.
    DeltaStructFieldMetadata meta =
        reloaded.getMetadata().getColumns().getFields().get(0).getMetadata();
    assertThat(meta.get("delta.columnMapping.id")).isEqualTo(1);
  }

  /**
   * Verifies that an unknown {@code delta.columnMapping.mode} value (e.g. "foo") stored on a table
   * causes the set-columns guard to reject the request with "Invalid column mapping mode".
   *
   * <p>The mode is seeded by setting the raw property via {@code set-properties} on an external
   * table (which accepts arbitrary properties), then sending {@code set-columns}; the stored mode
   * is read back and the guard fires.
   *
   * <p>The mode is the stored one, before the request's own property changes, so the same test also
   * checks that: a non-schema update is not blocked by the invalid stored mode; a request that sets
   * a valid mode does not repair it for its own set-columns; and a mode set or removed in the
   * request does not replace the stored one.
   */
  @Test
  public void testSetColumnsUsesStoredColumnMappingMode() throws Exception {
    Handle h = createDeltaExternal("tbl_invalid_cm_mode");
    // Seed an invalid mode in a separate earlier request, so it is already stored for the next one.
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

    // A non-schema update is not blocked by the stored invalid mode.
    updateTable(h2, new DeltaSetTableCommentUpdate().comment("still updatable"));
    Handle h3 =
        h2.withEtag(
            deltaTablesApi
                .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, h.name())
                .getMetadata()
                .getEtag());

    // Setting a valid mode in the same request does not repair the stored one for set-columns.
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                h3,
                new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", "name")),
                setColumns(fld("id", prim("long"), false, 1, "id"))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "Invalid column mapping mode");

    // The stored name mode applies, not the id mode the request sets: keeping the physical name
    // keeps the column's identity.
    Handle nameTable = createCmNameModeManaged("tbl_request_sets_mode");
    updateTable(
        nameTable,
        new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", "id")),
        setColumns(fld("a", prim("long"), false, null, "col-1")));

    // Removing the mode in the same request still checks the stored name mode.
    Handle removeMode = createCmNameModeManaged("tbl_request_removes_mode");
    TestUtils.assertDeltaApiException(
        () ->
            updateTable(
                removeMode,
                new DeltaRemovePropertiesUpdate().removals(List.of("delta.columnMapping.mode")),
                setColumns(fld("a", prim("long"), false, null, null))),
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "set-columns dropped column mapping metadata for column(s): a");
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
              cmMode("name"),
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
    // Names match ignoring case, so "id" and "ID" are the same logical name.
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

    // -------- Support: drop without column mapping --------
    // External (non-CM) table; set-columns removes "amount". Without column-mapping identities no
    // rename can be found, so this is a drop.
    {
      Handle h = createDeltaExternal("tbl_guards_g1");
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
                                  .metadata(new DeltaStructFieldMetadata())))));
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

    // -------- Support: ids without physical names on a table without column mapping --------
    // Non-CM table; incoming carries ids but no physicalName, so its columns are matched by name
    // and no rename can be found.
    {
      Handle h = createDeltaExternal("tbl_guards_g3");
      DeltaStructFieldMetadata idMeta = new DeltaStructFieldMetadata();
      idMeta.put("delta.columnMapping.id", 1); // no physicalName
      DeltaStructFieldMetadata amtMeta = new DeltaStructFieldMetadata();
      amtMeta.put("delta.columnMapping.id", 2); // no physicalName
      updateTable(
          h,
          cmMode("name"),
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
                  cmMode("name"),
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
                  cmMode("name"),
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
    // An unknown stored delta.columnMapping.mode is rejected on set-columns; that case is covered
    // by testSetColumnsUsesStoredColumnMappingMode.

    // -------- Negative control: same physicalName across different struct levels is allowed
    // --------
    // physicalName uniqueness is only required among SIBLINGS at the same level; the same
    // physicalName appearing in two DIFFERENT nested structs is fine.
    {
      DeltaStructField[] sameShape = {
        fld("s1", struct(fld("x", prim("long"), 3, "col-inner")), false, 1, "col-s1"),
        fld("s2", struct(fld("x", prim("long"), 4, "col-inner")), false, 2, "col-s2")
      };
      Handle h = createManaged("tbl_guards_phys_diff_levels", "name", 4, sameShape);
      DeltaLoadTableResponse r = updateTable(h, setColumns(sameShape));
      assertThat(columnNames(r)).containsExactly("s1", "s2");
    }

    // -------- Support: rename a column and re-add its old name in one request (name-mode) --------
    // Rename "a"→"x" (physicalName col-1 kept) and add a NEW column named "a" with a fresh
    // physicalName. The re-added "a" is a new column, not the renamed-away one.
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
                  cmMode("name"),
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

    // -------- Support: struct column changed to a scalar without column mapping --------
    // Non-CM external table with s: struct<x>. Changing "s" to a scalar drops nested field "x",
    // which is a drop and an add without column-mapping identities.
    {
      Handle h = createSimpleStructExternal("tbl_guards_struct_to_scalar");
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
                                  .metadata(new DeltaStructFieldMetadata())))));
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
    return createManaged(tableName, "name", 1, fld("a", prim("long"), false, 1, "col-1"));
  }

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: single top-level
   * struct column "s" (physicalName "col-s", id 1) containing nested field "x" (physicalName
   * "col-x", id 2). Used by the nested-rename tests.
   */
  private Handle createCmNameModeStructManaged(String tableName) throws ApiException {
    return createManaged(
        tableName,
        "name",
        2,
        fld("s", struct(fld("x", prim("long"), 2, "col-x")), false, 1, "col-s"));
  }

  /**
   * Creates a MANAGED Delta table with column-mapping id-mode enabled. Schema: single column "a"
   * (long, not null, delta.columnMapping.id 1, physicalName "col-1").
   */
  private Handle createCmIdModeManaged(String tableName) throws ApiException {
    return createManaged(tableName, "id", 1, fld("a", prim("long"), false, 1, "col-1"));
  }

  /**
   * Creates a MANAGED Delta table with column-mapping name-mode enabled. Schema: two columns "a"
   * (long, not null, id 1, physicalName "col-1") and "b" (string, nullable, id 2, physicalName
   * "col-2").
   */
  private Handle createTwoColCmNameModeManaged(String tableName) throws ApiException {
    return createManaged(
        tableName,
        "name",
        2,
        fld("a", prim("long"), false, 1, "col-1"),
        fld("b", prim("string"), 2, "col-2"));
  }

  /**
   * Creates a MANAGED name-mode Delta table with a single {@code map<string, struct<val>>} column
   * "props" (id 1, physicalName "col-props"); the value struct's field "val" has id 2, physicalName
   * "col-val". Used by the map-value nested rename test.
   */
  private Handle createCmNameModeMapStructManaged(String tableName) throws ApiException {
    return createManaged(
        tableName,
        "name",
        2,
        fld(
            "props",
            map(prim("string"), struct(fld("val", prim("string"), 2, "col-val"))),
            1,
            "col-props"));
  }

  /**
   * Creates a MANAGED name-mode Delta table with a single {@code array<struct<label>>} column
   * "events" (id 1, physicalName "col-events"); the element struct's field "label" has id 2,
   * physicalName "col-label". Used by the array-element nested rename test.
   */
  private Handle createCmNameModeArrayStructManaged(String tableName) throws ApiException {
    return createManaged(
        tableName,
        "name",
        2,
        fld(
            "events",
            array(struct(fld("label", prim("string"), 2, "col-label"))),
            1,
            "col-events"));
  }

  // -------------------------------------------------------- compact schema builders

  /** A column-mapping key sent with an explicit JSON null value (as opposed to left out). */
  private static final Object JSON_NULL = new Object();

  private static DeltaStructField fld(String name, DeltaDataType type, Object id, Object phys) {
    return fld(name, type, true, id, phys);
  }

  /**
   * A field with the given column-mapping id and physical name. A null {@code id} or {@code phys}
   * leaves that key out; {@link #JSON_NULL} sends it with a null value.
   */
  private static DeltaStructField fld(
      String name, DeltaDataType type, boolean nullable, Object id, Object phys) {
    DeltaStructFieldMetadata meta = new DeltaStructFieldMetadata();
    if (id != null) {
      meta.put("delta.columnMapping.id", id == JSON_NULL ? null : id);
    }
    if (phys != null) {
      meta.put("delta.columnMapping.physicalName", phys == JSON_NULL ? null : phys);
    }
    return new DeltaStructField().name(name).type(type).nullable(nullable).metadata(meta);
  }

  private static DeltaPrimitiveType prim(String type) {
    return new DeltaPrimitiveType().type(type);
  }

  private static DeltaStructType struct(DeltaStructField... fields) {
    return new DeltaStructType().type("struct").fields(List.of(fields));
  }

  private static DeltaArrayType array(DeltaDataType element) {
    return new DeltaArrayType().type("array").elementType(element).containsNull(true);
  }

  private static DeltaMapType map(DeltaDataType key, DeltaDataType value) {
    return new DeltaMapType().type("map").keyType(key).valueType(value).valueContainsNull(true);
  }

  private static DeltaSetSchemaUpdate setColumns(DeltaStructField... fields) {
    return new DeltaSetSchemaUpdate().columns(struct(fields));
  }

  /** Sets {@code delta.columnMapping.mode} in the same request. */
  private static DeltaSetPropertiesUpdate cmMode(String mode) {
    return new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", mode));
  }

  /** Creates a MANAGED Delta table in the given column-mapping mode with the given columns. */
  private Handle createManaged(
      String tableName, String mode, int maxColumnId, DeltaStructField... fields)
      throws ApiException {
    DeltaStagingTableResponse staging = createDeltaStaging(tableName);
    Map<String, String> properties =
        new HashMap<>(managedContractProperties(staging.getTableId().toString()));
    properties.put("delta.columnMapping.mode", mode);
    properties.put("delta.columnMapping.maxColumnId", Integer.toString(maxColumnId));
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(staging.getLocation())
                .tableType(DeltaTableType.MANAGED)
                .columns(struct(fields))
                .protocol(managedProtocol())
                .domainMetadata(
                    new DeltaDomainMetadataUpdates()
                        .deltaRowTracking(
                            new DeltaRowTrackingDomainMetadata().rowIdHighWaterMark(99L)))
                .lastCommitTimestampMs(PLACEHOLDER_LAST_COMMIT_TIMESTAMP_MS)
                .properties(properties));
    return new Handle(tableName, staging.getTableId(), resp.getMetadata().getEtag());
  }

  /** Creates an EXTERNAL Delta table with the given columns. */
  private Handle createExternal(String tableName, DeltaStructField... fields) throws Exception {
    String location = Files.createTempDirectory(testDirectoryRoot, "external_").toString();
    DeltaLoadTableResponse resp =
        deltaTablesApi.createTable(
            TestUtils.CATALOG_NAME,
            TestUtils.SCHEMA_NAME,
            new DeltaCreateTableRequest()
                .name(tableName)
                .location(location)
                .tableType(DeltaTableType.EXTERNAL)
                .columns(struct(fields))
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

  /** An EXTERNAL table with one struct column {@code s<x long>} and no column mapping. */
  private Handle createSimpleStructExternal(String tableName) throws Exception {
    return createExternal(
        tableName, fld("s", struct(fld("x", prim("long"), null, null)), false, null, null));
  }

  /** The field at {@code path}: a top-level column, then struct fields down to the last name. */
  private static DeltaStructField nestedField(DeltaLoadTableResponse r, String... path) {
    List<DeltaStructField> fields = r.getMetadata().getColumns().getFields();
    DeltaStructField current = null;
    for (int i = 0; i < path.length; i++) {
      String name = path[i];
      current = fields.stream().filter(x -> x.getName().equals(name)).findFirst().orElseThrow();
      if (i < path.length - 1) {
        fields = structFieldsOf(current.getType());
      }
    }
    return current;
  }

  /** Fields of a struct type, looking through an array element or a map value. */
  private static List<DeltaStructField> structFieldsOf(DeltaDataType type) {
    if (type instanceof DeltaArrayType a) {
      return structFieldsOf(a.getElementType());
    }
    if (type instanceof DeltaMapType m) {
      return structFieldsOf(m.getValueType());
    }
    assertThat(type).isInstanceOf(DeltaStructType.class);
    return ((DeltaStructType) type).getFields();
  }

  /** Field names of the struct reached by {@code path} (a top-level column, then struct fields). */
  private static List<String> nestedFieldNames(DeltaLoadTableResponse r, String... path) {
    List<DeltaStructField> fields = r.getMetadata().getColumns().getFields();
    for (String name : path) {
      DeltaStructField next =
          fields.stream().filter(x -> x.getName().equals(name)).findFirst().orElseThrow();
      fields = ((DeltaStructType) next.getType()).getFields();
    }
    return fields.stream().map(DeltaStructField::getName).collect(Collectors.toList());
  }

  // -------------------------------------------------------- table-driven set-columns cases

  /** Creates the table a {@link SchemaCase} runs against. */
  @FunctionalInterface
  private interface TableFactory {
    Handle create(SdkUpdateTableTest test, String tableName) throws Exception;
  }

  /**
   * One set-columns scenario: create a table, apply each setup request, then send {@code update}. A
   * null {@code rejection} expects success; otherwise the request fails with
   * INVALID_PARAMETER_VALUE and a message containing {@code rejection}.
   */
  private record SchemaCase(
      String name,
      TableFactory table,
      List<DeltaTableUpdate[]> setup,
      DeltaTableUpdate[] update,
      String rejection,
      List<String> columns,
      List<String> partitions,
      DeltaErrorType errorType) {
    @Override
    public String toString() {
      return name;
    }

    SchemaCase after(DeltaTableUpdate... step) {
      List<DeltaTableUpdate[]> steps = new ArrayList<>(setup);
      steps.add(step);
      return new SchemaCase(name, table, steps, update, rejection, columns, partitions, errorType);
    }

    SchemaCase expectColumns(String... names) {
      return new SchemaCase(
          name, table, setup, update, rejection, List.of(names), partitions, errorType);
    }

    /** Expects the rejection under {@code type} instead of INVALID_PARAMETER_VALUE. */
    SchemaCase asError(DeltaErrorType type) {
      return new SchemaCase(name, table, setup, update, rejection, columns, partitions, type);
    }

    SchemaCase expectPartitions(String... names) {
      return new SchemaCase(
          name, table, setup, update, rejection, columns, List.of(names), errorType);
    }
  }

  private static SchemaCase ok(String name, TableFactory table, DeltaTableUpdate... update) {
    return new SchemaCase(
        name,
        table,
        List.of(),
        update,
        null,
        null,
        null,
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION);
  }

  private static SchemaCase rejected(
      String name, String rejection, TableFactory table, DeltaTableUpdate... update) {
    return new SchemaCase(
        name,
        table,
        List.of(),
        update,
        rejection,
        null,
        null,
        DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION);
  }

  private void run(SchemaCase c) throws Exception {
    String tableName =
        "tbl_case_" + c.name().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
    Handle h = c.table().create(this, tableName);
    for (DeltaTableUpdate[] step : c.setup()) {
      h = h.withEtag(updateTable(h, step).getMetadata().getEtag());
    }
    Handle target = h;
    if (c.rejection() != null) {
      TestUtils.assertDeltaApiException(
          () -> updateTable(target, c.update()), c.errorType(), c.rejection());
      return;
    }
    DeltaLoadTableResponse r = updateTable(target, c.update());
    if (c.columns() != null) {
      assertThat(
              columnNames(
                  deltaTablesApi.loadTable(
                      TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, target.name())))
          .containsExactlyElementsOf(c.columns());
    }
    if (c.partitions() != null) {
      assertThat(r.getMetadata().getPartitionColumns()).containsExactlyElementsOf(c.partitions());
    }
  }

  private static final TableFactory EXTERNAL = (t, n) -> t.createDeltaExternal(n);
  private static final TableFactory EXTERNAL_STRUCT = (t, n) -> t.createSimpleStructExternal(n);
  private static final TableFactory NAME_A =
      (t, n) -> t.createManaged(n, "name", 1, fld("a", prim("long"), false, 1, "col-1"));
  private static final TableFactory ID_A =
      (t, n) -> t.createManaged(n, "id", 1, fld("a", prim("long"), false, 1, "col-1"));
  private static final TableFactory NAME_AB =
      (t, n) ->
          t.createManaged(
              n,
              "name",
              2,
              fld("a", prim("long"), false, 1, "col-1"),
              fld("b", prim("long"), 2, "col-2"));
  private static final TableFactory ID_AB =
      (t, n) ->
          t.createManaged(
              n,
              "id",
              2,
              fld("a", prim("long"), false, 1, "col-1"),
              fld("b", prim("long"), 2, "col-2"));
  private static final TableFactory NAME_S =
      (t, n) ->
          t.createManaged(
              n, "name", 2, fld("s", struct(fld("x", prim("long"), 2, "col-x")), 1, "col-s"));
  private static final TableFactory ID_S =
      (t, n) ->
          t.createManaged(
              n, "id", 2, fld("s", struct(fld("x", prim("long"), 2, "col-x")), 1, "col-s"));
  private static final TableFactory NAME_S2 =
      (t, n) ->
          t.createManaged(
              n,
              "name",
              3,
              fld(
                  "s",
                  struct(fld("x", prim("long"), 2, "col-x"), fld("z", prim("long"), 3, "col-z")),
                  1,
                  "col-s"));

  @ParameterizedTest(name = "{0}")
  @MethodSource("guardCases")
  public void testColumnMappingGuards(SchemaCase c) throws Exception {
    run(c);
  }

  @Test
  public void testJsonNullColumnMappingKeyIsSent() throws Exception {
    String body =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(fld("a", prim("long"), JSON_NULL, "col-1").getMetadata());
    assertThat(body).contains("\"delta.columnMapping.id\":null");
  }

  static Stream<SchemaCase> guardCases() {
    return Stream.of(
        // In name mode renames are found by physical name, ignoring case, so both count. y's id
        // changed, so an id key would find only one rename.
        rejected(
            "G27 two renames found by physical name in another case",
            "single top-level column rename",
            NAME_AB,
            setColumns(
                fld("x", prim("long"), false, 1, "COL-1"), fld("y", prim("long"), 9, "col-2"))),
        // A table without column mapping may add and reorder columns.
        ok(
                "G3 add a column without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("extra", prim("string"), null, null)))
            .expectColumns("id", "amount", "extra"),
        ok(
                "G4 reorder without column mapping",
                EXTERNAL,
                setColumns(
                    fld("amount", prim("double"), null, null),
                    fld("id", prim("long"), false, null, null)))
            .expectColumns("amount", "id"),
        // Without column-mapping identities a missing name is a drop and an add, as when the table
        // is replaced.
        ok(
                "G5 replace a column without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("flag", prim("boolean"), null, null)))
            .expectColumns("id", "flag"),
        // Mode none, but the stored fields still carry keys: they are matched by physical name.
        ok(
                "G6 rename on a mode-none table that stores keys",
                EXTERNAL,
                setColumns(
                    fld("ident", prim("long"), false, 1, "col-1"),
                    fld("amount", prim("double"), 2, "col-2")))
            .after(
                setColumns(
                    fld("id", prim("long"), false, 1, "col-1"),
                    fld("amount", prim("double"), 2, "col-2")))
            .expectColumns("ident", "amount"),
        // Nested renames and shape changes without column mapping.
        ok(
                "G34 nested struct rename without column mapping",
                EXTERNAL_STRUCT,
                setColumns(fld("s", struct(fld("y", prim("long"), null, null)), null, null)))
            .expectColumns("s"),
        ok(
                "G35 array element field rename without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("e", array(struct(fld("y", prim("long"), null, null))), null, null)))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("e", array(struct(fld("x", prim("long"), null, null))), null, null))),
        ok(
                "G36 map value field rename without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld(
                        "m",
                        map(prim("string"), struct(fld("y", prim("long"), null, null))),
                        null,
                        null)))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld(
                        "m",
                        map(prim("string"), struct(fld("x", prim("long"), null, null))),
                        null,
                        null))),
        ok(
                "G37 array of struct to scalar without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("e", prim("long"), null, null)))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("e", array(struct(fld("x", prim("long"), null, null))), null, null))),
        ok(
                "G38 map to scalar without column mapping",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("m", prim("long"), null, null)))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld(
                        "m",
                        map(prim("string"), struct(fld("x", prim("long"), null, null))),
                        null,
                        null))),
        // An explicit set-partition-columns next to a rename of another column.
        ok(
                "G1 rename a non-partition column with explicit partitions",
                NAME_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("c", prim("long"), 2, "col-2")),
                new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a")))
            .expectColumns("a", "c")
            .expectPartitions("a"),
        // Enabling on a table with a struct column, no rename.
        ok(
                "G2 enable with a nested struct",
                EXTERNAL_STRUCT,
                cmMode("name"),
                setColumns(fld("s", struct(fld("x", prim("long"), 2, "x")), false, 1, "s")))
            .expectColumns("s"),
        // Enabling needs a physical name on every column and nested field.
        rejected(
            "G7 enable without a physical name on one column",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"), fld("amount", prim("double"), 2, null))),
        rejected(
            "G8 enable with a null physical name",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 2, JSON_NULL))),
        rejected(
            "G9 enable with a numeric physical name",
            "requires a physical column name",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"), fld("amount", prim("double"), 2, 5))),
        rejected(
            "G10 enable without a nested physical name",
            "requires a physical column name",
            EXTERNAL_STRUCT,
            cmMode("name"),
            setColumns(fld("s", struct(fld("x", prim("long"), 2, null)), false, 1, "s"))),
        // Names differing only by case.
        rejected(
            "G18 top-level names differing only by case",
            "Duplicate field name",
            NAME_AB,
            setColumns(
                fld("a", prim("long"), false, 1, "col-1"), fld("A", prim("long"), 2, "col-2"))),
        rejected(
            "G32 nested names differing only by case",
            "Duplicate field name",
            NAME_S2,
            setColumns(
                fld(
                    "s",
                    struct(fld("x", prim("long"), 2, "col-x"), fld("X", prim("long"), 3, "col-z")),
                    1,
                    "col-s"))),
        // A key kept only below the top level is not "column mapping removed"; the per-column
        // check reports the column that lost it.
        rejected(
            "G28 key kept only in a struct field",
            "dropped column mapping metadata for column(s)",
            NAME_S,
            setColumns(fld("s", struct(fld("x", prim("long"), 2, "col-x")), null, null))),
        rejected(
            "G29 key kept in one of two struct fields",
            "dropped column mapping metadata for column(s)",
            NAME_S2,
            setColumns(
                fld(
                    "s",
                    struct(fld("x", prim("long"), 2, "col-x"), fld("z", prim("long"), null, null)),
                    null,
                    null))),
        rejected(
            "G30 key kept only in an array element field",
            "dropped column mapping metadata for column(s)",
            (t, n) ->
                t.createManaged(
                    n,
                    "name",
                    2,
                    fld("e", array(struct(fld("x", prim("long"), 2, "col-x"))), 1, "col-e")),
            setColumns(fld("e", array(struct(fld("x", prim("long"), 2, "col-x"))), null, null))),
        rejected(
            "G31 key kept only in a map value field",
            "dropped column mapping metadata for column(s)",
            (t, n) ->
                t.createManaged(
                    n,
                    "name",
                    2,
                    fld(
                        "m",
                        map(prim("string"), struct(fld("x", prim("long"), 2, "col-x"))),
                        1,
                        "col-m")),
            setColumns(
                fld(
                    "m",
                    map(prim("string"), struct(fld("x", prim("long"), 2, "col-x"))),
                    null,
                    null))),
        // Enabling with a shared id.
        rejected(
            "G11 enable with a shared id",
            "Duplicate column mapping id",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "id"),
                fld("amount", prim("double"), 1, "amount"))),
        // Enabling with a malformed id.
        rejected(
            "G12 enable with a boolean id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, true, "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "G13 enable with an object id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, Map.of(), "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "G14 enable with an empty-string id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, "", "id"),
                fld("amount", prim("double"), 2, "amount"))),
        rejected(
            "G15 enable with a null id",
            "malformed column mapping identity",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, JSON_NULL, "id"),
                fld("amount", prim("double"), 2, "amount"))),
        // Enabling with a shared physical name.
        rejected(
            "G16 enable with a shared physical name",
            "Duplicate column mapping physicalName",
            EXTERNAL,
            cmMode("name"),
            setColumns(
                fld("id", prim("long"), false, 1, "col-x"),
                fld("amount", prim("double"), 2, "col-x"))),
        // Duplicate physical names are compared without case.
        rejected(
            "G17 physical names differing only by case",
            "Duplicate column mapping physicalName",
            NAME_AB,
            setColumns(
                fld("a", prim("long"), false, 1, "col-dup"), fld("b", prim("long"), 2, "COL-DUP"))),
        // A column-mapped table needs usable identity values.
        rejected(
            "G19 empty physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, ""))),
        rejected(
            "G20 numeric physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, 5))),
        rejected(
            "G21 null physical name",
            "dropped column mapping metadata for column(s): a",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, JSON_NULL))),
        rejected(
            "G22 object physical name",
            "malformed column mapping identity",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, Map.of()))),
        rejected(
            "G23 empty-string id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, "", "col-1"))),
        rejected(
            "G24 object id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, Map.of(), "col-1"))),
        rejected(
            "G25 boolean id",
            "malformed column mapping identity",
            ID_A,
            setColumns(fld("a", prim("long"), false, true, "col-1"))),
        // Two empty physical names are malformed, not duplicates.
        rejected(
            "G26 two empty physical names",
            "malformed column mapping identity",
            NAME_AB,
            setColumns(fld("a", prim("long"), false, 1, ""), fld("b", prim("long"), 2, ""))),
        // An integer id sent as a string is usable.
        ok(
                "G33 integer id sent as a string",
                NAME_A,
                setColumns(fld("a", prim("long"), false, "1", "col-1")))
            .expectColumns("a"),
        // Ids are compared as text: 1 and "1" are one id, while "01" and "2147483648" are other
        // ids.
        rejected(
            "G45 ids 1 and \"1\"",
            "Duplicate column mapping id '1'",
            ID_A,
            setColumns(
                fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), "1", "col-b"))),
        ok(
                "G46 ids 01 and 2147483648 as strings",
                ID_A,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"),
                    fld("b", prim("long"), "01", "col-b"),
                    fld("c", prim("long"), "2147483648", "col-c")))
            .expectColumns("a", "b", "c"),
        // A table whose stored schema has no physical names is being enabled when the request
        // carries them. A rename is found only by a physical name that names a stored column, so
        // a new physical name is a drop and an add.
        ok(
                "G39 enable and rename with a new physical name",
                EXTERNAL,
                setColumns(
                    fld("id", prim("long"), false, 1, "id"),
                    fld("revenue", prim("double"), 2, "revenue")))
            .expectColumns("id", "revenue"),
        ok(
                "G40 enable and drop a column",
                EXTERNAL,
                setColumns(fld("id", prim("long"), false, 1, "id")))
            .expectColumns("id"),
        // The stored mode applies, not the one the request sets.
        ok(
                "G49 rename while removing column mapping",
                NAME_A,
                cmMode("none"),
                setColumns(fld("b", prim("long"), false, 1, "col-1")))
            .expectColumns("b"),
        ok(
                "G48 set id mode on a table without column mapping",
                EXTERNAL,
                cmMode("id"),
                setColumns(
                    fld("id", prim("long"), false, 1, "id"),
                    fld("amount", prim("double"), 2, "amount")))
            .expectColumns("id", "amount"),
        // Without keys in the request the columns are read by name.
        ok(
                "G41 enable without keys",
                EXTERNAL,
                cmMode("name"),
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null)))
            .expectColumns("id", "amount"),
        // A schema that has keys keeps them, even with no retained column.
        rejected(
            "G52 drop every column and the keys",
            "set-columns dropped column mapping metadata; removing column mapping",
            NAME_A,
            setColumns(fld("z", prim("long"), false, null, null))),
        // A stored column without keys has no identity to rename from, so it may gain them.
        ok(
                "G51 stored column without keys gains them",
                (t, n) -> t.createManaged(n, "name", 1, fld("a", prim("long"), false, null, null)),
                setColumns(fld("a", prim("long"), false, 1, "a")))
            .expectColumns("a"),
        // A new column may come without keys.
        ok(
                "G43 new column without keys",
                NAME_A,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("c", prim("long"), null, null)))
            .expectColumns("a", "c"),
        // Replacing every column with new ids and physical names is a drop and an add of each.
        ok(
                "G53 replace every column of a column-mapped table",
                NAME_AB,
                setColumns(fld("x", prim("long"), 3, "col-3"), fld("y", prim("long"), 4, "col-4")))
            .expectColumns("x", "y"),
        // In id mode renames are found by id.
        rejected(
            "G54 two renames in id mode",
            "single top-level column rename",
            (t, n) ->
                t.createManaged(
                    n,
                    "id",
                    2,
                    fld("a", prim("long"), false, 1, "col-1"),
                    fld("b", prim("long"), 2, "col-2")),
            setColumns(
                fld("x", prim("long"), false, 1, "col-1"), fld("y", prim("long"), 2, "col-2"))),
        // Enabling finds renames inside array elements and map values too.
        rejected(
                "G55 enable and rename an array element field",
                "separate statement",
                EXTERNAL,
                cmMode("name"),
                setColumns(
                    fld("id", prim("long"), false, 1, "id"),
                    fld("amount", prim("double"), 2, "amount"),
                    fld("e", array(struct(fld("y", prim("long"), 4, "x"))), 3, "e")))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld("e", array(struct(fld("x", prim("long"), null, null))), null, null))),
        rejected(
                "G56 enable and rename a map value field",
                "separate statement",
                EXTERNAL,
                cmMode("name"),
                setColumns(
                    fld("id", prim("long"), false, 1, "id"),
                    fld("amount", prim("double"), 2, "amount"),
                    fld("m", map(prim("string"), struct(fld("y", prim("long"), 4, "x"))), 3, "m")))
            .after(
                setColumns(
                    fld("id", prim("long"), false, null, null),
                    fld("amount", prim("double"), null, null),
                    fld(
                        "m",
                        map(prim("string"), struct(fld("x", prim("long"), null, null))),
                        null,
                        null))));
  }

  private static DeltaStructField withTypeChange(DeltaStructField field, String from, String to) {
    field.getMetadata().put("delta.typeChanges", List.of(Map.of("fromType", from, "toType", to)));
    return field;
  }

  /** Makes the stored type of one column unreadable. */
  private void corruptStoredType(Handle h, String columnName) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      Transaction tx = session.beginTransaction();
      session
          .createMutationQuery(
              "update ColumnInfoDAO c set c.typeJson = :json"
                  + " where c.table.id = :id and c.name = :name")
          .setParameter("json", "not json")
          .setParameter("id", h.tableId())
          .setParameter("name", columnName)
          .executeUpdate();
      tx.commit();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("kernelCases")
  public void testKernelValidatesRenames(SchemaCase c) throws Exception {
    run(c);
  }

  static Stream<SchemaCase> kernelCases() {
    return Stream.of(
        rejected(
            "K1 rename and type change",
            "Cannot change the type of existing field",
            NAME_A,
            setColumns(fld("b", prim("double"), false, 1, "col-1"))),
        rejected(
            "K2 same id, new physical name",
            "which is different from col-99",
            NAME_A,
            setColumns(fld("a", prim("long"), false, 1, "col-99"))),
        rejected(
            "K3 nested same id, new physical name",
            "which is different from col-z",
            NAME_S,
            setColumns(fld("s", struct(fld("x", prim("long"), 2, "col-z")), 1, "col-s"))),
        ok(
                "K4 same name with a new id is a drop and add",
                ID_A,
                setColumns(fld("a", prim("long"), false, 2, "col-1")))
            .expectColumns("a"),
        ok(
                "K5 nested same name with a new id is a drop and add",
                ID_S,
                setColumns(fld("s", struct(fld("x", prim("long"), 99, "col-x")), 1, "col-s")))
            .expectColumns("s"),
        rejected(
            "K6 nested rename with a sibling's new physical name",
            "which is different from col-x2",
            (t, n) ->
                t.createManaged(
                    n,
                    "name",
                    3,
                    fld(
                        "s",
                        struct(
                            fld("inner", prim("long"), 2, "col-in"),
                            fld("x", prim("long"), 3, "col-x")),
                        1,
                        "col-s")),
            setColumns(
                fld(
                    "s",
                    struct(
                        fld("renamed_inner", prim("long"), 2, "col-in"),
                        fld("x", prim("long"), 3, "col-x2")),
                    1,
                    "col-s"))),
        ok(
                "K7 rename a type-widened column",
                (t, n) ->
                    t.createManaged(
                        n,
                        "name",
                        1,
                        withTypeChange(
                            fld("a", prim("long"), false, 1, "col-1"), "integer", "long")),
                setColumns(
                    withTypeChange(fld("b", prim("long"), false, 1, "col-1"), "integer", "long")))
            .expectColumns("b"),
        ok(
                "K8 widen a type without a rename",
                (t, n) ->
                    t.createManaged(n, "name", 1, fld("a", prim("integer"), false, 1, "col-1")),
                setColumns(
                    withTypeChange(fld("a", prim("long"), false, 1, "col-1"), "integer", "long")))
            .expectColumns("a"),
        ok(
                "K9 nested rename plus a new field with the old name",
                (t, n) ->
                    t.createManaged(
                        n,
                        "name",
                        4,
                        fld("p", struct(fld("x", prim("long"), 2, "col-x")), 1, "col-p")),
                setColumns(
                    fld(
                        "p",
                        struct(
                            fld("y", prim("long"), 2, "col-x"),
                            fld("x", prim("long"), 5, "col-x2")),
                        1,
                        "col-p")))
            .expectColumns("p"),
        rejected(
            "K10 rename next to a stored column with an unreadable type",
            "the table schema cannot be read",
            (t, n) -> {
              Handle h =
                  t.createManaged(
                      n,
                      "name",
                      2,
                      fld("a", prim("long"), false, 1, "col-1"),
                      fld("v", prim("long"), 2, "col-v"));
              t.corruptStoredType(h, "v");
              return h;
            },
            setColumns(
                fld("b", prim("long"), false, 1, "col-1"), fld("v", prim("long"), 2, "col-v"))),
        rejected(
            "K11 nested rename keeps the id and drops the physical name",
            "Field y is missing physical name",
            NAME_S,
            setColumns(fld("s", struct(fld("y", prim("long"), 2, null)), 1, "col-s"))),
        rejected(
            "K12 id mode keeps the id with a new physical name",
            "which is different from col-new",
            ID_A,
            setColumns(fld("a", prim("long"), false, 1, "col-new"))),
        rejected(
            "K13 the stored maxColumnId applies, not the request's own set-properties",
            "fieldId <= maxFieldId",
            (t, n) ->
                t.createManaged(
                    n,
                    "name",
                    3,
                    fld("a", prim("long"), false, 1, "col-1"),
                    fld("d", prim("long"), 2, "col-d")),
            setColumns(
                fld("b", prim("long"), false, 1, "col-1"),
                fld("d", prim("long"), 2, "col-d"),
                fld("c", prim("long"), 3, "col-c")),
            new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.maxColumnId", "0"))),
        rejected(
            "K14 rename with a string column mapping id",
            "to be of type",
            NAME_A,
            setColumns(fld("b", prim("long"), false, "1", "col-1"))));
  }

  // -------------------------------------------------------- assertion helpers

  /** Returns the ordered list of top-level column names from a {@link DeltaLoadTableResponse}. */
  private static List<String> columnNames(DeltaLoadTableResponse resp) {
    return resp.getMetadata().getColumns().getFields().stream()
        .map(DeltaStructField::getName)
        .collect(Collectors.toList());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("partitionCases")
  public void testPartitionColumnFollowsRename(SchemaCase c) throws Exception {
    run(c);
  }

  static Stream<SchemaCase> partitionCases() {
    return Stream.of(
        ok(
                "Q1 rename one of two partition columns",
                NAME_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b2", prim("long"), 2, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("a", "b")))
            .expectPartitions("a", "b2"),
        rejected(
                "Q2 masked partition drop",
                "partition column 'b' dropped",
                NAME_AB,
                setColumns(fld("b", prim("long"), false, 1, "col-1")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b"))),
        rejected(
                "Q3 partition column without an identity",
                "has no column mapping identity",
                (t, n) ->
                    t.createManaged(
                        n,
                        "name",
                        1,
                        fld("a", prim("long"), false, 1, "col-1"),
                        fld("r", prim("long"), null, null)),
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"),
                    fld("r2", prim("long"), 2, "col-r2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("r"))),
        rejected(
                "Q4 strip a partition column's key",
                "dropped column mapping metadata for column(s): b",
                NAME_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), null, null)))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b"))),
        ok(
                "Q5 name mode partition follows the physical name",
                NAME_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b2", prim("long"), 9, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")))
            .expectPartitions("b2"),
        ok(
                "Q6 id mode partition follows the id",
                ID_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b2", prim("long"), 2, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")))
            .expectPartitions("b2"),
        rejected(
                "Q7 id mode same name with a new id",
                "partition column 'b' dropped",
                ID_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), 9, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b"))),
        ok(
                "Q8 first enable on a partitioned table",
                (t, n) ->
                    t.createManaged(
                        n,
                        "none",
                        0,
                        fld("a", prim("long"), false, null, null),
                        fld("b", prim("long"), null, null)),
                setColumns(fld("a", prim("long"), false, 1, "a"), fld("b", prim("long"), 2, "b")),
                new DeltaSetPropertiesUpdate().updates(Map.of("delta.columnMapping.mode", "name")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")))
            .expectPartitions("b"),
        rejected(
                "Q9 partition rename at a taken commit version",
                "Commit version already accepted",
                NAME_AB,
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b2", prim("long"), 2, "col-2")),
                new DeltaAddCommitUpdate().commit(commit(1L, "other-00000001.json")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")))
            .after(new DeltaAddCommitUpdate().commit(commit(1L, "00000001.json")))
            .asError(DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION),
        ok(
                "Q10 stored type of a non-partition column is unreadable",
                (t, n) -> {
                  Handle h = NAME_AB.create(t, n);
                  h =
                      h.withEtag(
                          t.updateTable(
                                  h,
                                  new DeltaSetPartitionColumnsUpdate()
                                      .partitionColumns(List.of("a")))
                              .getMetadata()
                              .getEtag());
                  t.corruptStoredType(h, "b");
                  return h;
                },
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), 2, "col-2")))
            .expectPartitions("a"),
        rejected(
                "Q11 no column mapping: the partition follows the logical name, not a leftover key",
                "partition column 'b' dropped",
                (t, n) ->
                    t.createManaged(
                        n,
                        "none",
                        2,
                        fld("a", prim("long"), false, 1, "col-1"),
                        fld("b", prim("long"), 2, "col-2")),
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b2", prim("long"), 2, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b"))),
        ok(
                "Q12 name mode, stored columns without keys: the partition resolves by name",
                (t, n) ->
                    t.createManaged(
                        n,
                        "name",
                        2,
                        fld("a", prim("long"), false, null, null),
                        fld("b", prim("long"), null, null)),
                setColumns(
                    fld("a", prim("long"), false, 1, "col-1"), fld("b", prim("long"), 2, "col-2")))
            .after(new DeltaSetPartitionColumnsUpdate().partitionColumns(List.of("b")))
            .expectPartitions("b"));
  }
}
