package io.unitycatalog.cli.credential;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.AzureServicePrincipalRequest;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.GcpServiceAccountKeyRequest;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CliCredentialOperations extends BaseCliOperations implements CredentialOperations {

  public CliCredentialOperations(ServerConfig config) {
    super("credential", config);
  }

  @Override
  public CredentialInfo createCredential(CreateCredentialRequest createCredentialRequest)
      throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--name", createCredentialRequest.getName()));
    if (createCredentialRequest.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createCredentialRequest.getComment());
    }
    addCloudIdentityArgs(
        argsList,
        createCredentialRequest.getAwsIamRole(),
        createCredentialRequest.getAzureServicePrincipal(),
        createCredentialRequest.getGcpServiceAccountKey());
    return execute(CredentialInfo.class, "create", argsList);
  }

  @Override
  public List<CredentialInfo> listCredentials(Optional<String> pageToken, CredentialPurpose purpose)
      throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (pageToken.isPresent()) {
      argsList.add("--page_token");
      argsList.add(pageToken.get());
    }
    return execute(new TypeReference<>() {}, "list", argsList);
  }

  @Override
  public CredentialInfo getCredential(String name) throws ApiException {
    return execute(CredentialInfo.class, "get", List.of("--name", name));
  }

  @Override
  public CredentialInfo updateCredential(
      String name, UpdateCredentialRequest updateCredentialRequest) throws ApiException {
    List<String> argsList = new ArrayList<>();
    if (updateCredentialRequest.getNewName() != null) {
      argsList.add("--new_name");
      argsList.add(updateCredentialRequest.getNewName());
    }
    if (updateCredentialRequest.getComment() != null) {
      argsList.add("--comment");
      argsList.add(updateCredentialRequest.getComment());
    }
    addCloudIdentityArgs(
        argsList,
        updateCredentialRequest.getAwsIamRole(),
        updateCredentialRequest.getAzureServicePrincipal(),
        updateCredentialRequest.getGcpServiceAccountKey());
    return executeUpdate(
        CredentialInfo.class,
        /* catchEmptyUpdateCliException= */ true,
        List.of("--name", name),
        argsList);
  }

  @Override
  public void deleteCredential(String name, Optional<Boolean> force) throws ApiException {
    List<String> argsList = new ArrayList<>(List.of("--name", name));
    if (force.isPresent()) {
      argsList.add("--force");
      argsList.add(force.get().toString());
    }
    execute(Void.class, "delete", argsList);
  }

  private void addCloudIdentityArgs(
      List<String> argsList,
      AwsIamRoleRequest awsIamRole,
      AzureServicePrincipalRequest servicePrincipal,
      GcpServiceAccountKeyRequest serviceAccountKey) {
    if (awsIamRole != null && awsIamRole.getRoleArn() != null) {
      argsList.add("--aws_iam_role_arn");
      argsList.add(awsIamRole.getRoleArn());
    }
    if (servicePrincipal != null) {
      addIfSet(argsList, "--azure_directory_id", servicePrincipal.getDirectoryId());
      addIfSet(argsList, "--azure_application_id", servicePrincipal.getApplicationId());
      addIfSet(argsList, "--azure_client_secret", servicePrincipal.getClientSecret());
    }
    if (serviceAccountKey != null) {
      argsList.add("--gcp_service_account_key_file");
      argsList.add(writeServiceAccountKeyFile(serviceAccountKey).toString());
    }
  }

  private static void addIfSet(List<String> argsList, String option, String value) {
    if (value != null) {
      argsList.add(option);
      argsList.add(value);
    }
  }

  /** Writes the key in the JSON layout the GCP console downloads, which is what the CLI reads. */
  private Path writeServiceAccountKeyFile(GcpServiceAccountKeyRequest key) {
    try {
      Path file = Files.createTempFile("uc-cli-test-service-account-key", ".json");
      file.toFile().deleteOnExit();
      objectMapper.writeValue(
          file.toFile(),
          Map.of(
              "type", "service_account",
              "client_email", key.getEmail(),
              "private_key_id", key.getPrivateKeyId(),
              "private_key", key.getPrivateKey()));
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
