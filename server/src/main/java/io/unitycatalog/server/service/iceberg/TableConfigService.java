package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Map;

/**
 * Builds the per-table FileIO configuration (credentials, region, etc.) returned to clients in the
 * Iceberg REST catalog's loadTable {@code config} field.
 */
public class TableConfigService {
  private final FileOperations fileOperations;

  public TableConfigService(FileOperations fileOperations) {
    this.fileOperations = fileOperations;
  }

  /**
   * Returns the FileIO config for the given table location. Note this vends temporary storage
   * credentials for the location as a side effect.
   *
   * @param location the registered table location used to scope credentials
   */
  public Map<String, String> getTableConfig(NormalizedURL location) {
    // TODO: metadataService.readTableMetadata called fileOperations.getFileIO already. It already
    //  generated this config but not passed back. For best efficiency the result from
    //  readTableMetadata should be reused.
    return fileOperations.getFileIOConfig(location);
  }
}
