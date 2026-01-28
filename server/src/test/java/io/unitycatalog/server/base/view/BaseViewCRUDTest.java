package io.unitycatalog.server.base.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ViewInfo;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Abstract base class that provides test cases for view CRUD operations in Unity Catalog.
 *
 * <p>This class extends {@link BaseViewCRUDTestEnv} and implements a test suite that verifies the
 * correct behavior of view operations including creation, retrieval, listing, and deletion.
 */
public abstract class BaseViewCRUDTest extends BaseViewCRUDTestEnv {

  @Test
  public void testViewCRUD() throws ApiException {
    // Verify view doesn't exist initially
    assertThatThrownBy(() -> viewOperations.getView(TestUtils.VIEW_FULL_NAME))
        .isInstanceOf(Exception.class);

    // Create and verify a view
    ViewInfo createdView = createAndVerifyView();

    // Get view and verify properties
    ViewInfo retrievedView = viewOperations.getView(TestUtils.VIEW_FULL_NAME);
    verifyViewInfo(retrievedView, createdView);

    // List views and verify
    List<ViewInfo> views =
        viewOperations.listViews(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(views).hasSize(1);
    assertThat(views.get(0).getName()).isEqualTo(TestUtils.VIEW_NAME);

    // Delete view
    testDeleteView();
  }

  @Test
  public void testViewNotFound() {
    assertThatThrownBy(() -> viewOperations.getView("nonexistent.schema.view"))
        .isInstanceOf(Exception.class);
  }

  @Test
  public void testDuplicateViewCreation() throws ApiException {
    // Create a view
    createAndVerifyView();

    // Attempt to create a view with the same name
    assertThatThrownBy(() -> createTestingView(TestUtils.VIEW_NAME, viewOperations))
        .isInstanceOf(Exception.class);

    // Cleanup
    viewOperations.deleteView(TestUtils.VIEW_FULL_NAME);
  }

  @Test
  public void testListMultipleViews() throws ApiException {
    // Create multiple views
    List<ViewInfo> createdViews = createMultipleTestingViews(5);

    // List and verify
    List<ViewInfo> views =
        viewOperations.listViews(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty());
    assertThat(views).hasSize(5);

    // Verify sorting
    List<ViewInfo> sortedViews = new ArrayList<>(views);
    assertThat(sortedViews).isSortedAccordingTo(Comparator.comparing(ViewInfo::getName));

    // Cleanup
    cleanUpViews(createdViews);
  }

  private void verifyViewInfo(ViewInfo retrievedView, ViewInfo expectedView) {
    assertThat(retrievedView.getViewId()).isEqualTo(expectedView.getViewId());
    assertThat(retrievedView.getName()).isEqualTo(expectedView.getName());
    assertThat(retrievedView.getCatalogName()).isEqualTo(expectedView.getCatalogName());
    assertThat(retrievedView.getSchemaName()).isEqualTo(expectedView.getSchemaName());
    assertThat(retrievedView.getColumns()).hasSize(2);
    assertThat(retrievedView.getRepresentations()).isNotEmpty();
  }

  private void testDeleteView() throws ApiException {
    viewOperations.deleteView(TestUtils.VIEW_FULL_NAME);
    assertThatThrownBy(() -> viewOperations.getView(TestUtils.VIEW_FULL_NAME))
        .isInstanceOf(Exception.class);
  }

  protected List<ViewInfo> createMultipleTestingViews(int numberOfViews) throws ApiException {
    List<ViewInfo> createdViews = new ArrayList<>();
    for (int i = 1; i <= numberOfViews; i++) {
      String viewName = TestUtils.VIEW_NAME + "_" + i;
      createdViews.add(createTestingView(viewName, viewOperations));
    }
    return createdViews;
  }

  private void cleanUpViews(List<ViewInfo> views) {
    views.forEach(
        view -> {
          try {
            viewOperations.deleteView(
                TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + view.getName());
          } catch (ApiException e) {
            // Ignore cleanup errors
          }
        });
  }
}
