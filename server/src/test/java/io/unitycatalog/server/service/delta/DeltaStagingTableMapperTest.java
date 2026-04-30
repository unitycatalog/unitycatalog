package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.delta.model.StagingTableResponse;
import io.unitycatalog.server.delta.model.TableType;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class DeltaStagingTableMapperTest {

  private static StagingTableInfo sampleStagingInfo() {
    return new StagingTableInfo()
        .id(UUID.randomUUID().toString())
        .stagingLocation("s3://bucket/staging/" + UUID.randomUUID())
        .name("tbl")
        .catalogName("cat")
        .schemaName("sch");
  }

  @Test
  public void testHappyPathMapping() {
    StagingTableInfo info = sampleStagingInfo();
    TemporaryCredentials creds =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId("AKIA")
                    .secretAccessKey("s3cret")
                    .sessionToken("tok"))
            .expirationTime(1700000000000L);

    StagingTableResponse resp = DeltaStagingTableMapper.toStagingTableResponse(info, creds);

    assertThat(resp.getTableId()).isEqualTo(UUID.fromString(info.getId()));
    assertThat(resp.getTableType()).isEqualTo(TableType.MANAGED);
    assertThat(resp.getLocation()).isEqualTo(info.getStagingLocation());
    assertThat(resp.getStorageCredentials()).hasSize(1);
    assertThat(resp.getRequiredProperties())
        .containsEntry(TableProperties.UC_TABLE_ID, info.getId());
  }

  /**
   * Pins the wire contract that null map values in {@code required-properties} and {@code
   * suggested-properties} survive ser + deser. The spec uses {@code null} to tell the client to
   * generate the value (e.g. the engine-managed inCommitTimestamp enablement version, or a
   * UUID-suffixed row-tracking column name). If Jackson drops those keys, the client cannot
   * distinguish "generate a value for this key" from "no constraint on this key".
   *
   * <p>Two hazards covered here:
   *
   * <ol>
   *   <li>The Delta REST response converter is configured with {@code NON_NULL}, which would
   *       normally omit null map values. The generated {@code StagingTableResponse} overrides this
   *       at the property level with {@code @JsonInclude(content = ALWAYS)}.
   *   <li>Jackson's default {@code Map} deserializer must preserve null values (it does, but
   *       regen'd code with {@code ACCEPT_EMPTY_STRING_AS_NULL_OBJECT} tweaks could change that).
   * </ol>
   */
  @Test
  public void testNullPropertyValuesSurviveRoundTrip() throws Exception {
    StagingTableResponse resp =
        DeltaStagingTableMapper.toStagingTableResponse(
            sampleStagingInfo(), new TemporaryCredentials());

    String json = DeltaRestCatalogMappers.MAPPER.writeValueAsString(resp);

    // 1. Serialization: the null values must appear as explicit "key": null in the JSON.
    assertThat(json)
        .contains("\"" + TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION + "\":null")
        .contains("\"" + TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP + "\":null")
        .contains("\"" + TableProperties.ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME + "\":null")
        .contains(
            "\""
                + TableProperties.ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME
                + "\":null");

    // 2. Deserialization: reading it back must give maps that still have the keys with null.
    StagingTableResponse roundTripped =
        DeltaRestCatalogMappers.MAPPER.readValue(json, StagingTableResponse.class);
    assertThat(roundTripped.getRequiredProperties())
        .containsEntry(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, null)
        .containsEntry(TableProperties.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP, null);
    assertThat(roundTripped.getSuggestedProperties())
        .containsEntry(TableProperties.ROW_TRACKING_MATERIALIZED_ROW_ID_COLUMN_NAME, null)
        .containsEntry(
            TableProperties.ROW_TRACKING_MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME, null)
        .containsEntry(TableProperties.ENABLE_ROW_TRACKING, "true");
  }
}
