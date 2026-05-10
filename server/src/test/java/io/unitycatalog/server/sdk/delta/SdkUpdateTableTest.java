package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.AssertEtag;
import io.unitycatalog.client.delta.model.AssertTableUUID;
import io.unitycatalog.client.delta.model.ClusteringDomainMetadata;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DomainMetadataUpdates;
import io.unitycatalog.client.delta.model.ErrorType;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.RemoveDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.RemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.RowTrackingDomainMetadata;
import io.unitycatalog.client.delta.model.SetDomainMetadataUpdate;
import io.unitycatalog.client.delta.model.SetPropertiesUpdate;
import io.unitycatalog.client.delta.model.SetProtocolUpdate;
import io.unitycatalog.client.delta.model.SetTableCommentUpdate;
import io.unitycatalog.client.delta.model.TableRequirement;
import io.unitycatalog.client.delta.model.TableUpdate;
import io.unitycatalog.client.delta.model.UpdateSnapshotVersionUpdate;
import io.unitycatalog.client.delta.model.UpdateTableRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
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

      DeltaProtocol newProtocol =
          new DeltaProtocol()
              .minReaderVersion(3)
              .minWriterVersion(7)
              .readerFeatures(List.of(TableFeature.V2_CHECKPOINT.specName()))
              .writerFeatures(
                  List.of(
                      TableFeature.V2_CHECKPOINT.specName(), TableFeature.ROW_TRACKING.specName()));
      DomainMetadataUpdates newDM =
          new DomainMetadataUpdates()
              .deltaClustering(
                  new ClusteringDomainMetadata().clusteringColumns(List.of(List.of("id"))));

      // Phase 1: every action except remove-domain-metadata. Splitting remove-DM into its own RPC
      // pins that the rowTracking entry only disappears as a result of remove-DM, not as a side
      // effect of phase 1's set-DM (which is intent-based per spec, not a full replacement).
      LoadTableResponse r1 =
          updateTable(
              h,
              new SetPropertiesUpdate().updates(Map.of("update_me", "v_new", "added", "v4")),
              new RemovePropertiesUpdate().removals(List.of("drop_me", "missing")),
              new SetProtocolUpdate().protocol(newProtocol),
              new SetDomainMetadataUpdate().updates(newDM),
              new SetTableCommentUpdate().comment("umbrella comment"));

      Map<String, String> props1 = r1.getMetadata().getProperties();
      assertThat(props1)
          .containsEntry("keep", "v1")
          .containsEntry("update_me", "v_new")
          .containsEntry("added", "v4")
          .doesNotContainKeys("drop_me", "missing");
      // set-protocol fully replaces delta.feature.* with entries derived from the new protocol;
      // pre-existing managed-contract features (catalogManaged etc.) must be cleared.
      assertThat(featurePropertiesIn(props1)).isEqualTo(featurePropertiesOf(newProtocol));
      // set-DM is additive: clustering JSON-encoded; rowTracking from create time still present.
      assertThat(props1)
          .containsEntry(TableProperties.CLUSTERING_COLUMNS, "[[\"id\"]]")
          .containsKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK);
      assertThat(r1.getMetadata().getEtag()).isNotEqualTo(h.etag());

      // Phase 2: remove-domain-metadata alone -- now the rowTracking entry must disappear.
      LoadTableResponse r2 =
          updateTable(
              h.withEtag(r1.getMetadata().getEtag()),
              new RemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking")));
      assertThat(r2.getMetadata().getProperties())
          .doesNotContainKey(TableProperties.ROW_TRACKING_ROW_ID_HIGH_WATER_MARK)
          .containsKey(TableProperties.CLUSTERING_COLUMNS);
    }

    // -------- update-metadata-snapshot-version --------
    {
      Handle external = createDeltaExternal("tbl_snapshot_external");

      // Missing required field on EXTERNAL -- run before the happy path so `external`'s etag is
      // still current (rejections don't mutate state, the happy path does). One case per missing
      // field so the per-field error message is exercised separately.
      TestUtils.assertDeltaApiException(
          () -> updateTable(external, new UpdateSnapshotVersionUpdate().lastCommitTimestampMs(1L)),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "requires last-commit-version");
      TestUtils.assertDeltaApiException(
          () -> updateTable(external, new UpdateSnapshotVersionUpdate().lastCommitVersion(1L)),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "requires last-commit-timestamp-ms");

      // Happy path on EXTERNAL.
      LoadTableResponse r =
          updateTable(
              external,
              new UpdateSnapshotVersionUpdate()
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
                  new UpdateSnapshotVersionUpdate()
                      .lastCommitVersion(1L)
                      .lastCommitTimestampMs(1700000000000L)),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "EXTERNAL");
    }

    // -------- assert-etag conflict: mutate once to roll the etag, then resubmit with the stale
    // value pinned explicitly so the conflict path actually fires.
    {
      Handle h = createDeltaManaged("tbl_etag_conflict", Map.of());
      updateTable(h, new SetPropertiesUpdate().updates(Map.of("k", "v")));
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h.name(),
                  requestWith(
                      h.tableId(),
                      Optional.of(h.etag()),
                      new SetPropertiesUpdate().updates(Map.of("x", "y")))),
          ErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION,
          "assert-etag failed");
    }

    // -------- request-shape rejections (share one table; none mutate state) --------
    {
      Handle h = createDeltaManaged("tbl_rejects", Map.of());
      SetPropertiesUpdate setKv = new SetPropertiesUpdate().updates(Map.of("k", "v"));

      // Missing assert-table-uuid: rejected during collectRequest.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h.name(),
                  new UpdateTableRequest().requirements(List.of()).updates(List.of(setKv))),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "assert-table-uuid requirement is required");

      // assert-table-uuid mismatch: rejected during checkRequirements.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h.name(), requestWith(UUID.randomUUID(), setKv)),
          ErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION,
          "assert-table-uuid failed");

      // Two same-typed actions in one request.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new SetPropertiesUpdate().updates(Map.of("a", "1")),
                  new SetPropertiesUpdate().updates(Map.of("b", "2"))),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "At most one set-properties is allowed per request");

      // set-properties and remove-properties touching the same key.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, setKv, new RemovePropertiesUpdate().removals(List.of("k"))),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-properties and remove-properties overlap");

      // set-domain-metadata and remove-domain-metadata touching the same domain.
      TestUtils.assertDeltaApiException(
          () ->
              updateTable(
                  h,
                  new SetDomainMetadataUpdate()
                      .updates(
                          new DomainMetadataUpdates()
                              .deltaRowTracking(
                                  new RowTrackingDomainMetadata().rowIdHighWaterMark(1L))),
                  new RemoveDomainMetadataUpdate().domains(List.of("delta.rowTracking"))),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-domain-metadata and remove-domain-metadata overlap");

      // Empty updates list.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h.name(), requestWith(h.tableId())),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "At least one update is required");

      // Path resolution fails before the UUID is checked, so the synthesized random UUID is fine.
      TestUtils.assertDeltaApiException(
          () -> updateTable("no_such_table", requestWith(UUID.randomUUID(), setKv)),
          ErrorType.NO_SUCH_TABLE_EXCEPTION,
          "Table not found");

      // set-protocol with null protocol -- rejected in applyUpdates.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new SetProtocolUpdate().protocol(null)),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-protocol requires a protocol");

      // set-domain-metadata with null updates block -- rejected in applyUpdates.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new SetDomainMetadataUpdate().updates(null)),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "set-domain-metadata requires an updates block");

      // remove-domain-metadata referencing an unknown domain name.
      TestUtils.assertDeltaApiException(
          () -> updateTable(h, new RemoveDomainMetadataUpdate().domains(List.of("delta.unknown"))),
          ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
          "Unknown domain in remove-domain-metadata");
    }
  }

  // ---------------------------------------------------------------- helpers

  /** Pins the update to the table's UUID and the Handle's etag. */
  private LoadTableResponse updateTable(Handle h, TableUpdate... updates) throws ApiException {
    return updateTable(h.name(), requestWith(h.tableId(), Optional.of(h.etag()), updates));
  }

  /** Escape hatch for tests that hand-build the request (custom requirements / empty updates). */
  private LoadTableResponse updateTable(String tableName, UpdateTableRequest request)
      throws ApiException {
    return deltaTablesApi.updateTable(
        TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, tableName, request);
  }

  /**
   * Build an {@link UpdateTableRequest} with the canonical {@code assert-table-uuid} requirement
   * and an optional {@code assert-etag} requirement. Empty {@code updates} is allowed for the
   * empty-list rejection case.
   */
  private static UpdateTableRequest requestWith(
      UUID assertUuid, Optional<String> assertEtag, TableUpdate... updates) {
    List<TableRequirement> requirements = new ArrayList<>();
    requirements.add(new AssertTableUUID().uuid(assertUuid));
    assertEtag.ifPresent(etag -> requirements.add(new AssertEtag().etag(etag)));
    return new UpdateTableRequest().requirements(requirements).updates(List.of(updates));
  }

  /** Convenience: assert-table-uuid only, no assert-etag. */
  private static UpdateTableRequest requestWith(UUID assertUuid, TableUpdate... updates) {
    return requestWith(assertUuid, Optional.empty(), updates);
  }
}
