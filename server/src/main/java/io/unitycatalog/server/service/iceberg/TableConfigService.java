package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;

/**
 * Builds the per-table FileIO configuration (credentials, region, etc.) returned to clients in the
 * Iceberg REST catalog's loadTable {@code config} field.
 */
public class TableConfigService {
  private final FileOperations fileOperations;

  public TableConfigService(FileOperations fileOperations) {
    this.fileOperations = fileOperations;
  }

  public Map<String, String> getTableConfig(
      TableMetadata tableMetadata, Set<CredentialContext.Privilege> privileges) {
    return fileOperations.getFileIOConfig(
        NormalizedURL.from(tableMetadata.location()), privileges);
  }
}
