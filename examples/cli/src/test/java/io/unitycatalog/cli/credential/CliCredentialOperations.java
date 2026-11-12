package io.unitycatalog.cli.credential;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
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
    if (createCredentialRequest.getAwsIamRole() != null
        && createCredentialRequest.getAwsIamRole().getRoleArn() != null) {
      argsList.add("--aws_iam_role_arn");
      argsList.add(createCredentialRequest.getAwsIamRole().getRoleArn());
    }
    AzureServicePrincipalRequest servicePrincipal =
        createCredentialRequest.getAzureServicePrincipal();
    if (servicePrincipal != null) {
      argsList.addAll(
          List.of(
              "--azure_directory_id",
              servicePrincipal.getDirectoryId(),
              "--azure_application_id",
              servicePrincipal.getApplicationId(),
              "--azure_client_secret",
              servicePrincipal.getClientSecret()));
    }
    if (createCredentialRequest.getGcpServiceAccountKey() != null) {
      argsList.add("--gcp_service_account_key_file");
      argsList.add(writeKeyFile(createCredentialRequest.getGcpServiceAccountKey()).toString());
    }
    return execute(CredentialInfo.class, "create", argsList);
  }

  /** Writes the key as the JSON key file that Google Cloud gives out for a service account. */
  private Path writeKeyFile(GcpServiceAccountKeyRequest key) {
    try {
      Path keyFile = Files.createTempFile("service-account-key", ".json");
      keyFile.toFile().deleteOnExit();
      Map<String, String> fields =
          Map.of(
              "type", "service_account",
              "client_email", key.getEmail(),
              "private_key_id", key.getPrivateKeyId(),
              "private_key", key.getPrivateKey());
      objectMapper.writeValue(keyFile.toFile(), fields);
      return keyFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
    if (updateCredentialRequest.getAwsIamRole() != null
        && updateCredentialRequest.getAwsIamRole().getRoleArn() != null) {
      argsList.add("--aws_iam_role_arn");
      argsList.add(updateCredentialRequest.getAwsIamRole().getRoleArn());
    }
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
}
