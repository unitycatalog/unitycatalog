package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.TableType;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

/**
 * Spark 4.0/4.1 lack the v2 {@code ViewCatalog}, so views can't be created via SQL DDL. All
 * assertions live in {@link AbstractViewReadIntegrationTest}; this subclass only supplies the
 * server-side (SDK) create/drop. The created view carries the same declared-vs-query-output column
 * difference and creation catalog/namespace as the SQL path, so the shared tests exercise the
 * {@code view.query.out.*} and {@code view.catalogAndNamespace.*} round-trips on the read-only
 * {@code buildV1ViewTable} path too.
 */
public class UCViewReadOnlyIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  @SneakyThrows
  protected void createView() {
    // The view.query.out.* properties and the declared columns below are spelled out one entry per
    // index, so they don't stay in sync with the fixture arrays automatically. Guard the expected
    // size (6): if QUERY_OUTPUT_COLUMNS / DECLARED_COLUMNS change, this fails loudly here -- update
    // both blocks -- rather than silently persisting a view that no longer matches VIEW_QUERY.
    assertThat(QUERY_OUTPUT_COLUMNS).hasSize(6);
    assertThat(DECLARED_COLUMNS).hasSize(6);

    tableOperations.createTable(
        new CreateTable()
            .name(VIEW_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.VIEW)
            .viewDefinition(VIEW_QUERY)
            // Omit view_dependencies (like the connector does for a plain view); the server accepts
            // an absent list.
            .properties(
                Map.of(
                    // Query-output names (differ from the declared columns below).
                    "view.query.out.numCols", Integer.toString(6),
                    "view.query.out.col.0", QUERY_OUTPUT_COLUMNS[0],
                    "view.query.out.col.1", QUERY_OUTPUT_COLUMNS[1],
                    "view.query.out.col.2", QUERY_OUTPUT_COLUMNS[2],
                    "view.query.out.col.3", QUERY_OUTPUT_COLUMNS[3],
                    "view.query.out.col.4", QUERY_OUTPUT_COLUMNS[4],
                    "view.query.out.col.5", QUERY_OUTPUT_COLUMNS[5],
                    // Creation catalog/namespace, so the unqualified `employees` resolves.
                    "view.catalogAndNamespace.numParts", "2",
                    "view.catalogAndNamespace.part.0", CATALOG_NAME,
                    "view.catalogAndNamespace.part.1", SCHEMA_NAME))
            // Declared columns matching VIEW_QUERY's projection: int, int, string, int,
            // array<string>, struct<id:int,name:string>.
            .columns(
                List.of(
                    column(DECLARED_COLUMNS[0], ColumnTypeName.INT, "int", "\"integer\"", 0),
                    column(DECLARED_COLUMNS[1], ColumnTypeName.INT, "int", "\"integer\"", 1),
                    column(DECLARED_COLUMNS[2], ColumnTypeName.STRING, "string", "\"string\"", 2),
                    column(DECLARED_COLUMNS[3], ColumnTypeName.INT, "int", "\"integer\"", 3),
                    column(
                        DECLARED_COLUMNS[4],
                        ColumnTypeName.ARRAY,
                        "array<string>",
                        "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}",
                        4),
                    column(
                        DECLARED_COLUMNS[5],
                        ColumnTypeName.STRUCT,
                        "struct<id:int,name:string>",
                        "{\"type\":\"struct\",\"fields\":["
                            + "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,"
                            + "\"metadata\":{}},"
                            + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,"
                            + "\"metadata\":{}}]}",
                        5))));
  }

  @Override
  @SneakyThrows
  protected void dropView() {
    tableOperations.deleteTable(VIEW_FULL_NAME);
  }

  private static ColumnInfo column(
      String name, ColumnTypeName typeName, String typeText, String dataTypeJson, int position) {
    return new ColumnInfo()
        .name(name)
        .typeName(typeName)
        .typeText(typeText)
        .typeJson(
            "{\"name\":\""
                + name
                + "\",\"type\":"
                + dataTypeJson
                + ",\"nullable\":true,\"metadata\":{}}")
        .nullable(true)
        .position(position);
  }
}
