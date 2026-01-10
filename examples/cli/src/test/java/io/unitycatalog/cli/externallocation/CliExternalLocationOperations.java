package io.unitycatalog.cli.externallocation;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.ExternalLocationInfo;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliExternalLocationOperations extends BaseCliOperations
    implements ExternalLocationOperations {

  public CliExternalLocationOperations(ServerConfig config) {
    super("external_location", config);
  }

  @Override
  public ExternalLocationInfo createExternalLocation(CreateExternalLocation createExternalLocation)
      throws ApiException {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "--name",
                createExternalLocation.getName(),
                "--url",
                createExternalLocation.getUrl(),
                "--credential_name",
                createExternalLocation.getCredentialName()));
    if (createExternalLocation.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createExternalLocation.getComment());
    }
    return execute(ExternalLocationInfo.class, "create", argsList);
  }

  @Override
  public List<ExternalLocationInfo> listExternalLocations(Optional<String> pageToken)
      throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public ExternalLocationInfo getExternalLocation(String name) throws ApiException {
    return execute(ExternalLocationInfo.class, "get", List.of("--name", name));
  }

  @Override
  public ExternalLocationInfo updateExternalLocation(
      String name, UpdateExternalLocation updateExternalLocation) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateExternalLocation.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateExternalLocation.getNewName());
    }
    if (updateExternalLocation.getUrl() != null) {
      argsList.add("--url");
      argsList.add(updateExternalLocation.getUrl());
    }
    if (updateExternalLocation.getCredentialName() != null) {
      argsList.add("--credential_name");
      argsList.add(updateExternalLocation.getCredentialName());
    }
    if (updateExternalLocation.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateExternalLocation.getComment());
    }
    return executeUpdate(ExternalLocationInfo.class, List.of("--name", name), argsList);
  }

  @Override
  public void deleteExternalLocation(String name, Optional<Boolean> force) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--name", name));
    if (force.isPresent() && force.get()) {
      argsList.add("--force");
      argsList.add("true");
    }
    execute(Void.class, "delete", argsList);
  }
}
