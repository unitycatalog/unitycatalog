package io.unitycatalog.cli.credential;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CliCredentialOperations implements CredentialOperations {
  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliCredentialOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public CredentialInfo createCredential(CreateCredentialRequest createCredentialRequest)
      throws ApiException {
    try {
      List<String> argsList =
          new ArrayList<>(
              List.of("credential", "create", "--name", createCredentialRequest.getName()));
      if (createCredentialRequest.getComment() != null) {
        argsList.add("--comment");
        argsList.add(createCredentialRequest.getComment());
      }
      if (createCredentialRequest.getAwsIamRole() != null
          && createCredentialRequest.getAwsIamRole().getRoleArn() != null) {
        argsList.add("--aws_iam_role_arn");
        argsList.add(createCredentialRequest.getAwsIamRole().getRoleArn());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode credentialInfoJson = executeCLICommand(args);
      return objectMapper.convertValue(credentialInfoJson, CredentialInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public List<CredentialInfo> listCredentials(Optional<String> pageToken, CredentialPurpose purpose)
      throws ApiException {
    try {
      List<String> argsList = new ArrayList<>(List.of("credential", "list"));
      if (pageToken.isPresent()) {
        argsList.add("--page_token");
        argsList.add(pageToken.get());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode credentialList = executeCLICommand(args);
      return objectMapper.convertValue(
          credentialList, new TypeReference<List<CredentialInfo>>() {});
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public CredentialInfo getCredential(String name) throws ApiException {
    try {
      String[] args = addServerAndAuthParams(List.of("credential", "get", "--name", name), config);
      JsonNode credentialInfoJson = executeCLICommand(args);
      return objectMapper.convertValue(credentialInfoJson, CredentialInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public CredentialInfo updateCredential(
      String name, UpdateCredentialRequest updateCredentialRequest) throws ApiException {
    try {
      List<String> argsList = new ArrayList<>(List.of("credential", "update", "--name", name));
      if (updateCredentialRequest.getNewName() != null) {
        argsList.add("--new_name");
        argsList.add(updateCredentialRequest.getNewName());
      }
      if (updateCredentialRequest.getComment() != null) {
        argsList.add("--comment");
        argsList.add(updateCredentialRequest.getComment());
      }
      if (updateCredentialRequest.getAwsIamRole() != null
          && updateCredentialRequest.getAwsIamRole().getRoleArn() != null) {
        argsList.add("--aws_iam_role_arn");
        argsList.add(updateCredentialRequest.getAwsIamRole().getRoleArn());
      }
      String[] args = addServerAndAuthParams(argsList, config);
      JsonNode updatedCredentialInfo = executeCLICommand(args);
      return objectMapper.convertValue(updatedCredentialInfo, CredentialInfo.class);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public void deleteCredential(String name) throws ApiException {
    try {
      String[] args =
          addServerAndAuthParams(List.of("credential", "delete", "--name", name), config);
      executeCLICommand(args);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ApiException) {
        throw (ApiException) e.getCause();
      }
      throw e;
    }
  }
}
