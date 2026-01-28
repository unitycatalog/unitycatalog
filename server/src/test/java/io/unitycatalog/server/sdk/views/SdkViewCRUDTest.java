package io.unitycatalog.server.sdk.views;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ViewsApi;
import io.unitycatalog.client.model.ListViewsResponse;
import io.unitycatalog.client.model.ViewInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.view.BaseViewCRUDTest;
import io.unitycatalog.server.base.view.ViewOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.Test;

public class SdkViewCRUDTest extends BaseViewCRUDTest {

  private ViewsApi localViewsApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected ViewOperations createViewOperations(ServerConfig config) {
    localViewsApi = new ViewsApi(TestUtils.createApiClient(config));
    return new SdkViewOperations(TestUtils.createApiClient(config));
  }

  @Test
  public void testListViewsWithNoNextPageTokenShouldReturnNull() throws Exception {
    ViewInfo testingView = createTestingView(TestUtils.VIEW_NAME, viewOperations);

    ListViewsResponse resp =
        localViewsApi.listViews(
            testingView.getCatalogName(), testingView.getSchemaName(), 100, null);

    assertThat(resp.getNextPageToken()).isNull();
    assertThat(resp.getViews()).hasSize(1);
    assertThat(resp.getViews().get(0).getName()).isEqualTo(TestUtils.VIEW_NAME);

    // Cleanup
    viewOperations.deleteView(TestUtils.VIEW_FULL_NAME);
  }

  @Test
  public void testGetViewWithNonExistentViewShouldFail() {
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(
            () ->
                localViewsApi.getView(
                    TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".nonexistent_view"))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }

  @Test
  public void testDeleteViewWithNonExistentViewShouldFail() {
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(
            () ->
                localViewsApi.deleteView(
                    TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + ".nonexistent_view"))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }

  @Test
  public void testViewWithInvalidSchemaNameShouldFail() {
    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(
            () -> localViewsApi.listViews(TestUtils.CATALOG_NAME, "nonexistent_schema", 100, null))
        .satisfies(
            ex -> assertThat(ex.getCode()).isEqualTo(ErrorCode.NOT_FOUND.getHttpStatus().code()))
        .withMessageContaining("not found");
  }

  @Test
  public void testViewDefinitionStoredCorrectly() throws Exception {
    ViewInfo createdView = createTestingView(TestUtils.VIEW_NAME, viewOperations);

    ViewInfo retrievedView = localViewsApi.getView(TestUtils.VIEW_FULL_NAME);

    assertThat(retrievedView.getRepresentations()).isNotEmpty();
    assertThat(retrievedView.getRepresentations().get(0).getDialect()).isEqualTo("spark");
    assertThat(retrievedView.getRepresentations().get(0).getSql()).contains("SELECT");

    // Cleanup
    viewOperations.deleteView(TestUtils.VIEW_FULL_NAME);
  }
}
