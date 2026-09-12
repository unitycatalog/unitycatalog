package io.unitycatalog.server.cleanup;

import io.unitycatalog.server.persist.utils.SimpleLocalFileIO;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.UriScheme;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileInfo;

/** Storage cleanup adapter for a local directory. */
public final class LocalStorageCleanupAdapter implements StorageCleanupAdapter {
  private final SimpleLocalFileIO fileIO = new SimpleLocalFileIO();
  private final String location;

  public LocalStorageCleanupAdapter(NormalizedURL location) {
    UriScheme scheme = UriScheme.fromURI(location.toUri());
    if (scheme != UriScheme.FILE && scheme != UriScheme.NULL) {
      throw new IllegalArgumentException("Local cleanup requires a local storage location");
    }
    this.location = location.toString();
  }

  @Override
  public List<String> listBatch(int maxFiles) {
    if (maxFiles <= 0) {
      throw new IllegalArgumentException("Maximum files must be positive");
    }
    List<String> batch = new ArrayList<>(maxFiles);
    try (CloseableIterable<FileInfo> files = fileIO.listPrefix(location)) {
      for (FileInfo file : files) {
        batch.add(file.location());
        if (batch.size() == maxFiles) {
          break;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close local storage listing", e);
    }
    return batch;
  }

  @Override
  public void deleteBatch(List<String> locations) {
    for (String file : locations) {
      try {
        fileIO.deleteFile(file);
      } catch (UncheckedIOException e) {
        if (!(e.getCause() instanceof NoSuchFileException)) {
          throw e;
        }
      }
    }
  }
}
