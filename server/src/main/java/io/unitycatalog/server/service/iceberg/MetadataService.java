package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.utils.NormalizedURL;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public class MetadataService {

  private final FileIOFactory fileIOFactory;

  public MetadataService(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  public TableMetadata readTableMetadata(String metadataLocation) {
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(metadataLocation));

    return CompletableFuture.supplyAsync(() -> TableMetadataParser.read(fileIO, metadataLocation))
        .join();
  }
}
