package io.unitycatalog.server.cleanup;

import java.util.List;

/** Lists and deletes materialized, bounded batches from one storage location. */
public interface StorageCleanupAdapter extends AutoCloseable {
  /**
   * Returns at most {@code maxFiles} file locations.
   *
   * @param maxFiles positive maximum number of files to return
   * @throws IllegalArgumentException if {@code maxFiles} is not positive
   */
  List<String> listBatch(int maxFiles);

  /** Deletes one batch returned by {@link #listBatch(int)}. */
  void deleteBatch(List<String> locations);

  @Override
  default void close() {}
}
