package io.unitycatalog.server.base.externallocation;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateExternalLocation;
import java.util.List;
import java.util.Optional;

public interface ExternalLocationOperations {
  ExternalLocationInfo createExternalLocation(CreateExternalLocation createExternalLocation)
      throws ApiException;

  List<ExternalLocationInfo> listExternalLocations(Optional<String> pageToken) throws ApiException;

  ExternalLocationInfo getExternalLocation(String name) throws ApiException;

  ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) throws ApiException;

  void deleteExternalLocation(String name) throws ApiException;
}
