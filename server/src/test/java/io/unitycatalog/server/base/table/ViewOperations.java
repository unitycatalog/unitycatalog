package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ReplaceView;
import io.unitycatalog.client.model.TableInfo;

public interface ViewOperations {
  TableInfo replaceView(String tableFullName, ReplaceView replaceView) throws ApiException;
}
