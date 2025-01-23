package io.unitycatalog.server.sdk.externallocation;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import java.util.List;
import java.util.Optional;

public class SdkExternalLocationOperations implements ExternalLocationOperations {
  private final ExternalLocationsApi externalLocationsApi;

  public SdkExternalLocationOperations(ApiClient apiClient) {
    this.externalLocationsApi = new ExternalLocationsApi(apiClient);
  }

  @Override
  public ExternalLocationInfo createExternalLocation(CreateExternalLocation createExternalLocation)
      throws ApiException {
    return externalLocationsApi.createExternalLocation(createExternalLocation);
  }

  @Override
  public List<ExternalLocationInfo> listExternalLocations(Optional<String> pageToken)
      throws ApiException {
    return externalLocationsApi
        .listExternalLocations(100, pageToken.orElse(null))
        .getExternalLocations();
  }

  @Override
  public ExternalLocationInfo getExternalLocation(String name) throws ApiException {
    return externalLocationsApi.getExternalLocation(name);
  }

  @Override
  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) throws ApiException {
    return externalLocationsApi.updateExternalLocation(name, updateExternalLocation);
  }

  @Override
  public void deleteExternalLocation(String name) throws ApiException {
    externalLocationsApi.deleteExternalLocation(name, true);
  }
}
