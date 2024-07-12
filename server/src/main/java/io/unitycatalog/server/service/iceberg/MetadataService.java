package io.unitycatalog.server.service.iceberg;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.net.URI;

public class MetadataService {

  private final FileIOFactory fileIOFactory;

  public MetadataService(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  public TableMetadata readTableMetadata(String metadataLocation) {
    URI metadataLocationUri = URI.create(metadataLocation);
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(metadataLocationUri);

    return TableMetadataParser.read(fileIO, metadataLocation);
  }
}
