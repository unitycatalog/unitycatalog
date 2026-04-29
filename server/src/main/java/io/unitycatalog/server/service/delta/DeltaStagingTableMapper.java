package io.unitycatalog.server.service.delta;

import io.unitycatalog.server.delta.model.CredentialOperation;
import io.unitycatalog.server.delta.model.StagingTableResponse;
import io.unitycatalog.server.delta.model.StagingTableResponseRequiredProtocol;
import io.unitycatalog.server.delta.model.StagingTableResponseSuggestedProtocol;
import io.unitycatalog.server.delta.model.TableType;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the Delta REST Catalog {@link StagingTableResponse} from the UC {@link StagingTableInfo}
 * plus freshly-vended cloud credentials. The required / suggested protocol and properties served
 * here are pulled from {@link UcManagedDeltaContract}, which is the single source of truth for the
 * UC catalog-managed Delta table contract; create / update / commit endpoints validate against that
 * same contract.
 */
public final class DeltaStagingTableMapper {

  private DeltaStagingTableMapper() {}

  /** Builds a {@link StagingTableResponse} from a freshly-created staging table + credentials. */
  public static StagingTableResponse toStagingTableResponse(
      StagingTableInfo info, TemporaryCredentials credentials) {
    var creds =
        DeltaCredentialsMapper.toCredentialsResponse(
            info.getStagingLocation(), credentials, CredentialOperation.READ_WRITE);

    Map<String, String> requiredProperties =
        new HashMap<>(UcManagedDeltaContract.REQUIRED_FIXED_PROPERTIES);
    // The rule-based property binds the Delta table to the UC-allocated UUID.
    requiredProperties.put(TableProperties.UC_TABLE_ID, info.getId());
    // Engine-generated entries: null on the wire signals "client substitutes at commit time."
    for (String key : UcManagedDeltaContract.ENGINE_GENERATED_PROPERTY_KEYS) {
      requiredProperties.put(key, null);
    }

    return new StagingTableResponse()
        .tableId(UUID.fromString(info.getId()))
        .tableType(TableType.MANAGED)
        .location(info.getStagingLocation())
        .storageCredentials(creds.getStorageCredentials())
        .requiredProtocol(
            new StagingTableResponseRequiredProtocol()
                .minReaderVersion(UcManagedDeltaContract.REQUIRED_MIN_READER_VERSION)
                .minWriterVersion(UcManagedDeltaContract.REQUIRED_MIN_WRITER_VERSION)
                .readerFeatures(UcManagedDeltaContract.REQUIRED_READER_FEATURES)
                .writerFeatures(UcManagedDeltaContract.REQUIRED_WRITER_FEATURES))
        .suggestedProtocol(
            new StagingTableResponseSuggestedProtocol()
                .readerFeatures(UcManagedDeltaContract.SUGGESTED_READER_FEATURES)
                .writerFeatures(UcManagedDeltaContract.SUGGESTED_WRITER_FEATURES))
        .requiredProperties(requiredProperties)
        .suggestedProperties(UcManagedDeltaContract.SUGGESTED_PROPERTIES);
  }
}
