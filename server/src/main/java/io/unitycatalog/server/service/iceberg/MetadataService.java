package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.NormalizedURL;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

/** Reads Iceberg table metadata for the Iceberg REST catalog's loadTable responses. */
public class MetadataService {

  private final FileOperations fileOperations;

  public MetadataService(FileOperations fileOperations) {
    this.fileOperations = fileOperations;
  }

  /**
   * Reads and parses the Iceberg {@link TableMetadata} at the given metadata-file location, using a
   * credential-vended {@link org.apache.iceberg.io.FileIO} obtained from {@link FileOperations}.
   *
   * @param metadataLocation the normalized location of the {@code *.metadata.json} file
   */
  public TableMetadata readTableMetadata(NormalizedURL metadataLocation) {
    // TODO: cache fileIO
    try (FileIO fileIO = fileOperations.getFileIO(metadataLocation)) {
      return TableMetadataParser.read(fileIO, metadataLocation.toString());
    }
  }
}
