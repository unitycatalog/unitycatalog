package io.unitycatalog.server.service.iceberg;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MetadataService {

  private final FileIOFactory fileIOFactory;

  public MetadataService() {
    fileIOFactory = new FileIOFactory();
  }

  public TableMetadata readTableMetadata(String metadataLocation) throws IOException {
    URI metadataLocationUri = URI.create(metadataLocation);
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(metadataLocationUri);

    TableMetadata tableMetadata;
    if (fileIO == null) {
      String metadataJson = new String(Files.readAllBytes(Paths.get(metadataLocationUri)));
      tableMetadata = TableMetadataParser.fromJson(metadataLocation, metadataJson);
    } else {
      tableMetadata = TableMetadataParser.read(fileIO, metadataLocation);
    }

    return tableMetadata;
  }
}
