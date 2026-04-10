package io.unitycatalog.spark;

import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Abstraction over the UC table API surface. Two implementations coexist behind a feature flag:
 * {@link LegacyUCBackend} (the original UC REST API) and {@link DeltaRestBackend} (the new Delta
 * REST Catalog API at /api/2.1/unity-catalog/delta/v1).
 *
 * <p>Methods declare {@code throws Exception} because the underlying API clients throw checked
 * {@code ApiException} and the Spark catalog layer throws checked {@code NoSuchTableException}. The
 * callers are Scala, which does not enforce checked exceptions.
 */
public interface CatalogBackend {

  String apiPathLabel();

  Identifier[] listTables(String catalogName, String schemaName) throws Exception;

  Table loadTable(
      String catalogName,
      Identifier ident,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      boolean serverSidePlanningEnabled,
      String ucUri)
      throws Exception;

  Table createTable(
      String catalogName,
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties)
      throws Exception;

  boolean dropTable(String catalogName, Identifier ident) throws Exception;

  void renameTable(String catalogName, Identifier from, Identifier to) throws Exception;

  Map<String, String> stageManagedTableAndGetProps(
      String catalogName,
      Identifier ident,
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception;

  Map<String, String> prepareReplaceProps(
      String catalogName,
      Identifier ident,
      String existingTableLocation,
      String existingTableId,
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception;

  Map<String, String> prepareExternalTableProps(
      Map<String, String> properties,
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      String ucUri)
      throws Exception;

  Optional<ExistingTableInfo> resolveExistingTable(
      String catalogName, Identifier ident, boolean allowMissing) throws Exception;
}
