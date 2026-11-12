package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaRemovePropertiesUpdate;
import io.unitycatalog.client.delta.model.DeltaSetPropertiesUpdate;
import io.unitycatalog.client.delta.model.DeltaTableUpdate;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.delta.DeltaBaseTableCRUDTestEnv;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Delta REST updateTable on a server with {@code
 * server.managed-table.uniform-iceberg-v2.allow-missing-dv=true}, where {@code
 * delta.enableIcebergCompatV2} decides whether a MANAGED table may drop the deletion-vector
 * property.
 */
public class SdkUpdateTableAllowMissingDvTest extends DeltaBaseTableCRUDTestEnv {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(
        ServerProperties.Property.UNIFORM_ICEBERG_V2_ALLOW_MISSING_DV.getKey(), "true");
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  public void testReplayOfContractInputCommitAfterLaterChangeReturnsNewestState() throws Exception {
    Handle h =
        createDeltaManaged(
            "tbl_replay_contract_input", Map.of("delta.enableIcebergCompatV2", "true"));
    DeltaTableUpdate[] v1 = {
      new DeltaAddCommitUpdate().commit(commit(1L, "00000001.json")),
      new DeltaRemovePropertiesUpdate().removals(List.of("delta.enableDeletionVectors"))
    };
    DeltaLoadTableResponse r1 = updateTable(h, v1);
    DeltaLoadTableResponse r2 =
        updateTable(
            h.withEtag(r1.getMetadata().getEtag()),
            new DeltaAddCommitUpdate().commit(commit(2L, "00000002.json")),
            new DeltaRemovePropertiesUpdate().removals(List.of("delta.enableIcebergCompatV2")),
            new DeltaSetPropertiesUpdate().updates(Map.of("delta.enableDeletionVectors", "true")));

    DeltaLoadTableResponse replay = updateTable(h, v1);

    assertThat(replay.getLatestTableVersion()).isEqualTo(2L);
    assertThat(replay.getMetadata().getProperties())
        .isEqualTo(r2.getMetadata().getProperties())
        .containsEntry("delta.enableDeletionVectors", "true")
        .doesNotContainKey("delta.enableIcebergCompatV2");
  }
}
