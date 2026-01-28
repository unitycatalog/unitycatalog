package io.unitycatalog.server.base.view;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.ViewInfo;
import io.unitycatalog.client.model.ViewRepresentation;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract base class that provides the test environment setup for view CRUD operations.
 *
 * <p>This class extends {@link BaseCRUDTest} and serves as a foundation for testing view-related
 * operations in Unity Catalog.
 */
public abstract class BaseViewCRUDTestEnv extends BaseCRUDTest {

  protected SchemaOperations schemaOperations;
  protected ViewOperations viewOperations;
  protected String schemaId;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract ViewOperations createViewOperations(ServerConfig serverConfig);

  protected static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("id")
              .typeText("INTEGER")
              .typeJson("{\"type\": \"integer\"}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .comment("ID column")
              .nullable(false),
          new ColumnInfo()
              .name("name")
              .typeText("VARCHAR(255)")
              .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
              .typeName(ColumnTypeName.STRING)
              .position(1)
              .comment("Name column")
              .nullable(true));

  protected static final List<ViewRepresentation> VIEW_DEFINITION =
      List.of(
          new ViewRepresentation()
              .dialect("spark")
              .sql(
                  "SELECT id, name FROM "
                      + TestUtils.CATALOG_NAME
                      + "."
                      + TestUtils.SCHEMA_NAME
                      + ".source_table"));

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    viewOperations = createViewOperations(serverConfig);
    createCommonResources();
  }

  private void createCommonResources() {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    try {
      catalogOperations.createCatalog(createCatalog);
      SchemaInfo schemaInfo =
          schemaOperations.createSchema(
              new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
      schemaId = schemaInfo.getSchemaId();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  protected ViewInfo createAndVerifyView() {
    ViewInfo viewInfo = createTestingView(TestUtils.VIEW_NAME, viewOperations);
    assertThat(viewInfo.getName()).isEqualTo(TestUtils.VIEW_NAME);
    assertThat(viewInfo.getCatalogName()).isEqualTo(TestUtils.CATALOG_NAME);
    assertThat(viewInfo.getSchemaName()).isEqualTo(TestUtils.SCHEMA_NAME);
    assertThat(viewInfo.getViewId()).isNotNull();
    assertThat(viewInfo.getRepresentations()).isNotEmpty();
    return viewInfo;
  }

  @SneakyThrows
  public static ViewInfo createTestingView(String viewName, ViewOperations viewOperations) {
    CreateView createViewRequest =
        new CreateView()
            .name(viewName)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .viewDefinition(VIEW_DEFINITION)
            .comment(TestUtils.COMMENT);

    return viewOperations.createView(createViewRequest);
  }
}
