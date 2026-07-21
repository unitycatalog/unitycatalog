package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

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

  public ViewMetadata readViewMetadata(String metadataLocation) {
    FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(metadataLocation));
    return CompletableFuture.supplyAsync(
            () -> ViewMetadataParser.read(fileIO.newInputFile(metadataLocation)))
        .join();
  }

  public void writeViewMetadata(ViewMetadata viewMetadata, String metadataLocation) {
    FileIO fileIO =
        fileIOFactory.getFileIO(
            NormalizedURL.from(metadataLocation),
            Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE));
    ViewMetadataParser.overwrite(viewMetadata, fileIO.newOutputFile(metadataLocation));
  }
}
