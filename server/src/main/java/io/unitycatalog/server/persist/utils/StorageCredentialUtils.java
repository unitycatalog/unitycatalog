package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.model.AwsIamRoleRequest;
import io.unitycatalog.server.model.AwsIamRoleResponse;
import io.unitycatalog.server.model.AzureManagedIdentityRequest;
import io.unitycatalog.server.model.AzureManagedIdentityResponse;

public class StorageCredentialUtils {
  public static AwsIamRoleResponse fromAwsIamRoleRequest(AwsIamRoleRequest awsIamRoleRequest) {
    // TODO: add external id and unity catalog server iam role
    return new AwsIamRoleResponse().roleArn(awsIamRoleRequest.getRoleArn());
  }

  public static AzureManagedIdentityResponse fromAzureManagedIdentityRequest(
      AzureManagedIdentityRequest azureManagedIdentityRequest, String credentialId) {
    return new AzureManagedIdentityResponse()
        .managedIdentityId(azureManagedIdentityRequest.getManagedIdentityId())
        .accessConnectorId(azureManagedIdentityRequest.getAccessConnectorId())
        .credentialId(credentialId);
  }
}
