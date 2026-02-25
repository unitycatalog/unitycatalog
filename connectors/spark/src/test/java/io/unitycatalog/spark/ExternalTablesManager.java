package io.unitycatalog.spark;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Manages temporary external table storage locations for Spark integration tests.
 *
 * <p>This class creates a temporary directory structure to simulate external table
 * storage and provides methods to generate storage locations for catalog, schema,
 * and table combinations. It implements {@link AutoCloseable} to ensure proper
 * cleanup of temporary resources.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ExternalTablesManager manager = new ExternalTablesManager()) {
 *     String location = manager.getLocation("catalog1", "schema1", "table1");
 *     // Use the location for testing
 * } // Automatic cleanup
 * }</pre>
 *
 * <p>Thread-safety: This class is not thread-safe. External synchronization is
 * required if instances are accessed from multiple threads.
 */
public class ExternalTablesManager implements AutoCloseable {
  private final Path externalTablesDir;
  private volatile boolean closed = false;

  /**
   * Creates a new ExternalTablesManager with a temporary directory for storing
   * external table data.
   *
   * <p>The temporary directory is created with the prefix "spark-external-tables"
   * in the system's default temporary directory.
   *
   * @throws RuntimeException if the temporary directory cannot be created
   */
  public ExternalTablesManager() {
    try {
      externalTablesDir = Files.createTempDirectory("spark-external-tables");
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temporary directory for external tables", e);
    }
  }

  /**
   * Generates a storage location path for the specified catalog, schema, and table.
   *
   * <p>The location follows the pattern: {@code <temp-dir>/<catalog>/<schema>/<table>}
   *
   * @param catalog the catalog name (must not be null or empty)
   * @param schema the schema name (must not be null or empty)
   * @param table the table name (must not be null or empty)
   * @return the absolute path string for the table's storage location
   * @throws IllegalStateException if this manager has been closed
   * @throws IllegalArgumentException if any parameter is null or empty
   */
  public String getLocation(String catalog, String schema, String table) {
    ensureNotClosed();
    validateParameter(catalog, "catalog");
    validateParameter(schema, "schema");
    validateParameter(table, "table");

    return externalTablesDir.resolve(catalog).resolve(schema).resolve(table).toString();
  }

  /**
   * Generates a storage location path for a fully qualified table name.
   *
   * <p>The fully qualified table name must be in the format {@code catalog.schema.table}.
   * The location follows the pattern: {@code <temp-dir>/<catalog>/<schema>/<table>}
   *
   * @param fullTableName the fully qualified table name in format "catalog.schema.table"
   * @return the absolute path string for the table's storage location
   * @throws IllegalStateException if this manager has been closed
   * @throws IllegalArgumentException if the table name is null, empty, or not properly formatted
   */
  public String getLocation(String fullTableName) {
    ensureNotClosed();
    validateParameter(fullTableName, "fullTableName");

    String[] parts = fullTableName.split("\\.", -1);
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          "fullTableName must be in format 'catalog.schema.table', got: " + fullTableName);
    }

    String catalog = parts[0].trim();
    String schema = parts[1].trim();
    String table = parts[2].trim();

    if (catalog.isEmpty() || schema.isEmpty() || table.isEmpty()) {
      throw new IllegalArgumentException(
          "catalog, schema, and table parts must not be empty in: " + fullTableName);
    }

    return getLocation(catalog, schema, table);
  }

  /**
   * Returns the root directory path used for external table storage.
   *
   * @return the path to the temporary directory
   * @throws IllegalStateException if this manager has been closed
   */
  public Path getExternalTablesDirectory() {
    ensureNotClosed();
    return externalTablesDir;
  }

  /**
   * Cleans up the temporary directory and all its contents.
   *
   * <p>This method recursively deletes all files and subdirectories within
   * the temporary directory, then deletes the directory itself. After calling
   * this method, the manager cannot be used anymore.
   *
   * <p>This method is idempotent and can be called multiple times safely.
   *
   * @deprecated Use {@link #close()} instead for AutoCloseable support
   */
  @Deprecated
  public void cleanUp() {
    close();
  }

  /**
   * Closes this manager and cleans up all temporary resources.
   *
   * <p>Recursively deletes the temporary directory and all its contents.
   * This method is idempotent and can be called multiple times safely.
   * Any subsequent calls to {@link #getLocation(String, String, String)} will
   * throw an {@link IllegalStateException}.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    if (externalTablesDir != null && Files.exists(externalTablesDir)) {
      try {
        deleteRecursively(externalTablesDir);
      } catch (IOException e) {
        // Log warning but don't throw - cleanup is best-effort
        System.err.println("Warning: Failed to clean up temporary directory: "
            + externalTablesDir + " - " + e.getMessage());
      }
    }
  }

  /**
   * Recursively deletes a directory and all its contents.
   *
   * @param path the path to delete
   * @throws IOException if an I/O error occurs during deletion
   */
  private void deleteRecursively(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Validates that a parameter is not null or empty.
   *
   * @param value the value to validate
   * @param paramName the parameter name for error messages
   * @throws IllegalArgumentException if the value is null or empty
   */
  private void validateParameter(String value, String paramName) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(paramName + " must not be null or empty");
    }
  }

  /**
   * Ensures this manager has not been closed.
   *
   * @throws IllegalStateException if this manager has been closed
   */
  private void ensureNotClosed() {
    if (closed) {
      throw new IllegalStateException("ExternalTablesManager has been closed");
    }
  }
}
