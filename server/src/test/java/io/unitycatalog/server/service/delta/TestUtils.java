package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaProtocol;
import io.unitycatalog.server.service.delta.DeltaConsts.TableFeature;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** Shared test fixtures for UC-managed Delta table tests. */
final class TestUtils {

  private TestUtils() {}

  /** Protocol with all required UC-managed features except deletionVectors. */
  static DeltaProtocol protocolWithoutDv() {
    return new DeltaProtocol()
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
  }

  /** Properties satisfying all required entries except delta.enableDeletionVectors. */
  static Map<String, String> propertiesWithoutDv() {
    Map<String, String> props = new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    props.remove(TableProperties.ENABLE_DELETION_VECTORS);
    props.put(TableProperties.UC_TABLE_ID, UUID.randomUUID().toString());
    return props;
  }

  /** Same as propertiesWithoutDv() but includes delta.enableIcebergCompatV2=true. */
  static Map<String, String> propertiesWithoutDvAndWithIcebergCompatV2() {
    Map<String, String> props = propertiesWithoutDv();
    props.put(TableProperties.ENABLE_ICEBERG_COMPAT_V2, "true");
    return props;
  }

  /** ServerProperties with server.managed-table.uniform-iceberg-v2.allow-missing-dv=true. */
  static ServerProperties serverPropertiesWithAllowMissingDv() {
    Properties props = new Properties();
    props.setProperty(
        ServerProperties.Property.UNIFORM_ICEBERG_V2_ALLOW_MISSING_DV.getKey(), "true");
    return new ServerProperties(props);
  }
}
