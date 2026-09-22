package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.utils.CooperativeDeadline;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;

/** Deletes one bound location in batches with cooperative cancellation checks. */
public final class InterruptiblePrefixOperations implements SupportsPrefixOperations {
  private static final int MAX_BATCH_SIZE = 1000;

  private final DelegateFileIO delegate;
  private final String boundPrefix;
  private final CooperativeDeadline deadline;

  /** Creates prefix operations bound to one location and sharing the given deadline. */
  public InterruptiblePrefixOperations(
      DelegateFileIO delegate, String boundPrefix, CooperativeDeadline deadline) {
    this.boundPrefix = Objects.requireNonNull(boundPrefix, "boundPrefix");
    if (!boundPrefix.endsWith("/")) {
      throw new IllegalArgumentException("Bound prefix must end with '/'");
    }
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.deadline = Objects.requireNonNull(deadline, "deadline");
  }

  @Override
  public InputFile newInputFile(String path) {
    throw unsupportedOperation();
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw unsupportedOperation();
  }

  @Override
  public void deleteFile(String path) {
    throw unsupportedOperation();
  }

  /**
   * Returns a lazy listing for the exact bound prefix.
   *
   * @throws IllegalArgumentException if {@code prefix} is not the bound prefix
   * @throws CancellationException if interrupted or the deadline has been reached
   */
  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    checkPrefix(prefix);
    deadline.checkCancelled();
    return delegate.listPrefix(prefix);
  }

  /**
   * Deletes objects matching the bound slash-terminated prefix in bounded batches. An object at the
   * prefix without its trailing slash is not included.
   *
   * @throws IllegalArgumentException if {@code prefix} is not the bound prefix
   * @throws CancellationException if interrupted or the deadline has been reached
   */
  @Override
  public void deletePrefix(String prefix) {
    checkPrefix(prefix);
    deadline.checkCancelled();
    Iterator<FileInfo> listing = delegate.listPrefix(prefix).iterator();
    while (true) {
      deadline.checkCancelled();
      List<String> batch = new ArrayList<>(MAX_BATCH_SIZE);
      while (batch.size() < MAX_BATCH_SIZE) {
        deadline.checkCancelled();
        if (!listing.hasNext()) {
          break;
        }
        batch.add(listing.next().location());
      }
      deadline.checkCancelled();
      if (batch.isEmpty()) {
        return;
      }
      delegate.deleteFiles(batch);
    }
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    throw unsupportedOperation();
  }

  @Override
  public void close() {
    delegate.close();
  }

  /**
   * Rejects any prefix other than the bound prefix. Exact matching prevents an object-store prefix
   * such as {@code .../id/} from reaching a sibling such as {@code .../id2/}.
   */
  private void checkPrefix(String prefix) {
    if (!boundPrefix.equals(prefix)) {
      throw new IllegalArgumentException("Prefix does not match bound prefix");
    }
  }

  private static UnsupportedOperationException unsupportedOperation() {
    return new UnsupportedOperationException("Only bound prefix operations are supported");
  }
}
