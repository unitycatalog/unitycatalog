package io.unitycatalog.server.base.view;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateView;
import io.unitycatalog.client.model.ViewInfo;
import java.util.List;
import java.util.Optional;

public interface ViewOperations {
  ViewInfo createView(CreateView createViewRequest) throws ApiException;

  List<ViewInfo> listViews(String catalogName, String schemaName, Optional<String> pageToken)
      throws ApiException;

  ViewInfo getView(String viewFullName) throws ApiException;

  void deleteView(String viewFullName) throws ApiException;
}
