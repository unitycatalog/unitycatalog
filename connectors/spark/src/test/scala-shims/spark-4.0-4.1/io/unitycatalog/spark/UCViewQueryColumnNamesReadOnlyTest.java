package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Spark 4.0/4.1 can only read views (no v2 {@code ViewCatalog}), and {@code buildV1ViewTable}
 * reconstructs a v1 VIEW {@code CatalogTable} from the UC row. This pins that the view's persisted
 * query-output column names ({@code view.query.out.*}) -- the names the SELECT list produces, which
 * can differ from the declared column names -- are carried through rather than regenerated from the
 * declared columns. Regenerating breaks Spark's by-name view-column matching with
 * INCOMPATIBLE_VIEW_SCHEMA_CHANGE (the same bug the Spark 4.2 {@code UCJoinViewE2ETest} covers on
 * the create+read path; here the view is created server-side since 4.0/4.1 cannot create it).
 */
public class UCViewQueryColumnNamesReadOnlyTest extends BaseSparkIntegrationTest {

  private static final String SRC_TABLE = "src_tbl";
  private static final String VIEW = "renamed_col_view";

  private String tbl(String name) {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + name;
  }

  /** Registers an external parquet source table so the view's query can resolve against it. */
  @SneakyThrows
  private void createSourceTable() {
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name(SRC_TABLE)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.PARQUET)
                .storageLocation(Files.createTempDirectory("uc_src").toUri().toString())
                .columns(
                    List.of(
                        new ColumnInfo()
                            .name("name")
                            .typeName(ColumnTypeName.STRING)
                            .typeText("string")
                            .typeJson(
                                "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,"
                                    + "\"metadata\":{}}")
                            .nullable(true)
                            .position(0))));
  }

  /**
   * Creates a VIEW whose declared column is {@code emp} but whose query output is {@code name}
   * ({@code SELECT name FROM src_tbl}), with the {@code view.query.out.*} properties Spark persists
   * for such a view. This is the shape that triggers the bug when the connector regenerates the
   * query-output names from the declared columns.
   */
  @SneakyThrows
  private void createRenamedColumnView() {
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name(VIEW)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.VIEW)
                .viewDefinition("SELECT name FROM " + tbl(SRC_TABLE))
                .viewDependencies(new DependencyList().dependencies(List.of()))
                .properties(
                    Map.of(
                        "view.query.out.numCols", "1",
                        "view.query.out.col.0", "name",
                        "view.catalogAndNamespace.numParts", "2",
                        "view.catalogAndNamespace.part.0", CATALOG_NAME,
                        "view.catalogAndNamespace.part.1", SCHEMA_NAME))
                .columns(
                    List.of(
                        new ColumnInfo()
                            .name("emp")
                            .typeName(ColumnTypeName.STRING)
                            .typeText("string")
                            .typeJson(
                                "{\"name\":\"emp\",\"type\":\"string\",\"nullable\":true,"
                                    + "\"metadata\":{}}")
                            .nullable(true)
                            .position(0))));
  }

  @Test
  public void testReadViewWithRenamedColumnResolvesThroughSpark() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createSourceTable();
    createRenamedColumnView();

    // Reading the view resolves its stored query text against the base table and matches the query
    // output column `name` to the declared view column `emp` via the persisted query-output names.
    // The declared name is what the reader sees.
    assertThat(sql("SELECT emp FROM %s", tbl(VIEW))).isEmpty();
    assertThat(sql("DESCRIBE %s", tbl(VIEW)))
        .anyMatch(row -> "emp".equals(row.getString(0)));
  }
}
