package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.UpdateView;

public interface ViewOperations {
  TableInfo updateView(String tableFullName, UpdateView updateView) throws ApiException;
}
