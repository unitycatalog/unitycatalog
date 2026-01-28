package io.unitycatalog.server.sdk.views;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ViewsApi;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.ViewInfo;
import io.unitycatalog.server.base.view.ViewOperations;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SdkViewOperations implements ViewOperations {
  private final ViewsApi viewsApi;

  public SdkViewOperations(ApiClient apiClient) {
    this.viewsApi = new ViewsApi(apiClient);
  }

  @Override
  public ViewInfo createView(CreateView createViewRequest) throws ApiException {
    return viewsApi.createView(createViewRequest);
  }

  @Override
  public List<ViewInfo> listViews(String catalogName, String schemaName, Optional<String> pageToken)
      throws ApiException {
    return Objects.requireNonNull(
        viewsApi.listViews(catalogName, schemaName, 100, pageToken.orElse(null)).getViews());
  }

  @Override
  public ViewInfo getView(String viewFullName) throws ApiException {
    return viewsApi.getView(viewFullName);
  }

  @Override
  public void deleteView(String viewFullName) throws ApiException {
    viewsApi.deleteView(viewFullName);
  }
}
