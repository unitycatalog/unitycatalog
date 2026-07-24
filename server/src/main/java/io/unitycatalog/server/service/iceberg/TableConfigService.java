package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Map;
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

  /**
   * Returns the FileIO config for the given table. Note this vends temporary storage credentials
   * for the table's location as a side effect.
   *
   * @param tableMetadata the Iceberg table metadata whose location is used to scope credentials
   */
  public Map<String, String> getTableConfig(TableMetadata tableMetadata) {
    // TODO: metadataService.readTableMetadata called fileOperations.getFileIO already. It already
    //  generated this config but not passed back. For best efficiency the result from
    //  readTableMetadata should be reused.
    return fileOperations.getFileIOConfig(NormalizedURL.from(tableMetadata.location()));
  }
}
