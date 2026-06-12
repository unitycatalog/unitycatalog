package io.unitycatalog.server.service.iceberg;

import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.NormalizedURL;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;

public class MetadataService {

  private static final Set<CredentialContext.Privilege> READ_WRITE =
      Set.of(CredentialContext.Privilege.SELECT, CredentialContext.Privilege.UPDATE);

  // Matches the version prefix of metadata file names written by this service and by other
  // Iceberg catalog implementations, e.g. "00003-<uuid>.metadata.json" or "v3.metadata.json".
  private static final Pattern METADATA_FILE_VERSION =
      Pattern.compile("^(?:v)?(\\d+)-?.*\\.metadata\\.json(?:\\.gz)?$");

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

  public void writeTableMetadata(TableMetadata tableMetadata, String metadataLocation) {
    FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(metadataLocation), READ_WRITE);

    CompletableFuture.runAsync(
            () -> TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(metadataLocation)))
        .join();
  }

  /** Best-effort cleanup of a metadata file that lost a commit race or whose commit failed. */
  public void deleteTableMetadata(String metadataLocation) {
    try {
      FileIO fileIO = fileIOFactory.getFileIO(NormalizedURL.from(metadataLocation), READ_WRITE);
      CompletableFuture.runAsync(() -> fileIO.deleteFile(metadataLocation)).join();
    } catch (Exception e) {
      // Orphaned metadata files are harmless; the commit outcome is decided by the catalog.
    }
  }

  /** Builds the location of the next metadata file, following the common Iceberg naming scheme. */
  public static String newMetadataLocation(TableMetadata tableMetadata, int version) {
    String metadataDirectory =
        tableMetadata
            .properties()
            .getOrDefault(
                TableProperties.WRITE_METADATA_LOCATION, tableMetadata.location() + "/metadata");
    return String.format("%s/%05d-%s.metadata.json", metadataDirectory, version, UUID.randomUUID());
  }

  /**
   * Parses the version number out of a metadata file location, returning -1 when the file name does
   * not follow a recognized versioning scheme (so the next version becomes 0).
   */
  public static int parseMetadataVersion(String metadataLocation) {
    String fileName = metadataLocation.substring(metadataLocation.lastIndexOf('/') + 1);
    Matcher matcher = METADATA_FILE_VERSION.matcher(fileName);
    if (matcher.matches()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        return -1;
      }
    }
    return -1;
  }
}
