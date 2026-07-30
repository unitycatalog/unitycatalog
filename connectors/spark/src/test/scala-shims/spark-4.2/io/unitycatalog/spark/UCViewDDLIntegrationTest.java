package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.nio.file.Files;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Spark 4.2 exposes the v2 {@code ViewCatalog}, so CREATE / SHOW / DROP VIEW all route to Unity
 * Catalog. Complements the shared read guarantee with the full DDL round-trip.
 */
public class UCViewDDLIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  protected void createView() {
    sql("CREATE VIEW %s AS %s", VIEW_FULL_NAME, VIEW_QUERY);
  }

  @Test
  public void testShowViewsListsViewAndDropRemovesIt() {
    createSessionAndView();
    assertThat(sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME))
        .anyMatch(row -> VIEW_NAME.equals(row.getString(1)));

    sql("DROP VIEW %s", VIEW_FULL_NAME);
    assertThat(sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME))
        .noneMatch(row -> VIEW_NAME.equals(row.getString(1)));
  }

  /** A plain view's dependencies, derived from the query text, are persisted on the server. */
  @Test
  public void testCreateViewPersistsDerivedDependencies() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String srcFullName = CATALOG_NAME + "." + SCHEMA_NAME + ".dep_src";
    createExternalTable("dep_src");
    sql("CREATE VIEW %s AS SELECT * FROM %s", VIEW_FULL_NAME, srcFullName);

    TableInfo view = getServerTable(VIEW_FULL_NAME);
    assertThat(view.getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .contains(srcFullName);
  }

  /** A case-differing CTE reference is not leaked as a dependency; only the base table is kept. */
  @Test
  public void testCreateViewDerivesDependenciesWithCaseInsensitiveCte() {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String srcFullName = CATALOG_NAME + "." + SCHEMA_NAME + ".dep_src";
    createExternalTable("dep_src");
    sql(
        "CREATE VIEW %s AS WITH v_cte AS (SELECT * FROM %s) SELECT * FROM V_CTE",
        VIEW_FULL_NAME, srcFullName);

    assertThat(getServerTable(VIEW_FULL_NAME).getViewDependencies().getDependencies())
        .extracting(dependency -> dependency.getTable().getTableFullName())
        .containsExactly(srcFullName);
  }

  /** Registers an external parquet table server-side so it is resolvable by Spark's analyzer. */
  @SneakyThrows
  private void createExternalTable(String name) {
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name(name)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.PARQUET)
                .storageLocation(Files.createTempDirectory("uc_" + name).toUri().toString())
                .columns(
                    List.of(
                        new ColumnInfo()
                            .name("c")
                            .typeName(ColumnTypeName.INT)
                            .typeText("int")
                            .typeJson(
                                "{\"name\":\"c\",\"type\":\"integer\",\"nullable\":true,"
                                    + "\"metadata\":{}}")
                            .nullable(true)
                            .position(0))));
  }

  @SneakyThrows
  private TableInfo getServerTable(String fullName) {
    return new SdkTableOperations(createApiClient(serverConfig)).getTable(fullName);
  }
}
