package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * Spark 4.0/4.1 lack the v2 {@code ViewCatalog}, so views can't be created via SQL. A view created
 * server-side must still be readable (shared base) and surface on the table listing.
 */
public class UCViewReadOnlyIntegrationTest extends AbstractViewReadIntegrationTest {

  @Override
  @SneakyThrows
  protected void createView() {
    new SdkTableOperations(createApiClient(serverConfig))
        .createTable(
            new CreateTable()
                .name(VIEW_NAME)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .tableType(TableType.VIEW)
                .viewDefinition(VIEW_QUERY)
                .viewDependencies(new DependencyList().dependencies(List.of()))
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

  @Test
  public void testViewAppearsUnderShowTables() {
    createSessionAndView();
    assertThat(sql("SHOW TABLES IN %s.%s", CATALOG_NAME, SCHEMA_NAME))
        .anyMatch(row -> VIEW_NAME.equals(row.getString(1)));
  }
}
