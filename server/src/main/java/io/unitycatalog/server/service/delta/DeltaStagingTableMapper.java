package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.DeltaCredentialOperation;
import io.unitycatalog.server.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.server.delta.model.DeltaStagingTableResponseRequiredProtocol;
import io.unitycatalog.server.delta.model.DeltaStagingTableResponseSuggestedProtocol;
import io.unitycatalog.server.delta.model.DeltaTableType;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the UC Delta API {@link DeltaStagingTableResponse} from the UC {@link StagingTableInfo}
 * plus freshly-vended cloud credentials. The required / suggested protocol and properties served
 * here are pulled from {@link UcManagedDeltaContract}, which is the single source of truth for the
 * UC catalog-managed Delta table contract; create / update / commit endpoints validate against that
 * same contract.
 */
public final class DeltaStagingTableMapper {

  private DeltaStagingTableMapper() {}

  /**
   * Builds a {@link DeltaStagingTableResponse} from a freshly-created staging table + credentials.
   */
  public static DeltaStagingTableResponse toStagingTableResponse(
      StagingTableInfo info, TemporaryCredentials credentials, ServerProperties serverProperties) {
    var creds =
        DeltaCredentialsMapper.toCredentialsResponse(
            info.getStagingLocation(), credentials, DeltaCredentialOperation.READ_WRITE);

    Map<String, String> requiredProperties =
        new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    // When allow-missing-dv is enabled, omit DV from required so UniForm tables without DVs can
    // be created. Commit-time validation in UcManagedDeltaContract.validate() still enforces DV
    // for non-UniForm tables (skipDv=false path).
    if (serverProperties.isUniformIcebergV2AllowMissingDv()) {
      requiredProperties.remove(TableProperties.ENABLE_DELETION_VECTORS);
    }
    // The rule-based property binds the Delta table to the UC-allocated UUID.
    requiredProperties.put(TableProperties.UC_TABLE_ID, info.getId());
    // Engine-generated entries: null on the wire signals "client substitutes at commit time."
    for (String key : UcManagedDeltaContract.ENGINE_GENERATED_PROPERTY_KEYS) {
      requiredProperties.put(key, null);
    }

    return new DeltaStagingTableResponse()
        .tableId(UUID.fromString(info.getId()))
        .tableType(DeltaTableType.MANAGED)
        .location(info.getStagingLocation())
        .storageCredentials(creds.getStorageCredentials())
        .requiredProtocol(
            new DeltaStagingTableResponseRequiredProtocol()
                .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                .writerFeatures(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES))
        .suggestedProtocol(
            new DeltaStagingTableResponseSuggestedProtocol()
                .readerFeatures(UcManagedDeltaContract.SUGGESTED_READER_FEATURES)
                .writerFeatures(UcManagedDeltaContract.SUGGESTED_WRITER_FEATURES))
        .requiredProperties(requiredProperties)
        .suggestedProperties(UcManagedDeltaContract.SUGGESTED_PROPERTIES);
  }
}
