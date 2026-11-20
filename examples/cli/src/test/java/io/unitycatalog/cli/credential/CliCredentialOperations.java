package io.unitycatalog.cli.credential;

import com.fasterxml.jackson.core.type.TypeReference;
import io.unitycatalog.cli.BaseCliOperations;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CredentialInfo;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.S3CompatibleRequest;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.credential.CredentialOperations;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
    addS3CompatibleArgs(argsList, createCredentialRequest.getS3Compatible());
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
    if (updateCredentialRequest.getAwsIamRole() != null
        && updateCredentialRequest.getAwsIamRole().getRoleArn() != null) {
      argsList.add("--aws_iam_role_arn");
      argsList.add(updateCredentialRequest.getAwsIamRole().getRoleArn());
    }
    addS3CompatibleArgs(argsList, updateCredentialRequest.getS3Compatible());
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

  private static void addS3CompatibleArgs(List<String> argsList, S3CompatibleRequest s3Compatible) {
    if (s3Compatible == null) {
      return;
    }
    Map<String, Object> args = new LinkedHashMap<>();
    args.put("--s3_access_key_id", s3Compatible.getAccessKeyId());
    args.put("--s3_secret_access_key", s3Compatible.getSecretAccessKey());
    args.put("--s3_endpoint", s3Compatible.getS3Endpoint());
    if (s3Compatible.getAssumeRole() != null) {
      args.put("--s3_sts_endpoint", s3Compatible.getAssumeRole().getStsEndpoint());
      args.put("--s3_role_arn", s3Compatible.getAssumeRole().getRoleArn());
      args.put("--s3_sts_region", s3Compatible.getAssumeRole().getStsRegion());
    }
    args.forEach(
        (option, value) -> {
          if (value != null) {
            argsList.add(option);
            argsList.add(value.toString());
          }
        });
  }
}
