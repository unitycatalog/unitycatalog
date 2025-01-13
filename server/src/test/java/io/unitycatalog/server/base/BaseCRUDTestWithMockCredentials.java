package io.unitycatalog.server.base;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import java.time.Instant;
import java.util.Date;
import software.amazon.awssdk.services.sts.model.Credentials;

public abstract class BaseCRUDTestWithMockCredentials extends BaseCRUDTest {
  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.put("s3.bucketPath.0", "s3://test-bucket0");
    serverProperties.put("s3.accessKey.0", "accessKey0");
    serverProperties.put("s3.secretKey.0", "secretKey0");
    serverProperties.put("s3.sessionToken.0", "sessionToken0");

    serverProperties.put("gcs.bucketPath.0", "gs://test-bucket0");
    serverProperties.put("gcs.jsonKeyFilePath.0", "testing://0");

    serverProperties.put("adls.storageAccountName.0", "test-bucket0");
    serverProperties.put("adls.tenantId.0", "tenantId0");
    serverProperties.put("adls.clientId.0", "clientId0");
    serverProperties.put("adls.clientSecret.0", "clientSecret0");
  }

  @Override
  protected void setUpCredentialOperations() {
    awsCredentialVendor = mock(AwsCredentialVendor.class);
    Credentials credentials =
        Credentials.builder()
            .accessKeyId("test-access-key-id")
            .secretAccessKey("test-secret-access-key")
            .sessionToken("test-session-token")
            .build();
    doReturn(credentials).when(awsCredentialVendor).vendAwsCredentials(any());

    azureCredentialVendor = mock(AzureCredentialVendor.class);
    AzureCredential azureCredential =
        AzureCredential.builder()
            .sasToken("test-sas-token")
            .expirationTimeInEpochMillis(System.currentTimeMillis() + 6000)
            .build();
    doReturn(azureCredential).when(azureCredentialVendor).vendAzureCredential(any());

    gcpCredentialVendor = mock(GcpCredentialVendor.class);
    AccessToken accessToken =
        new AccessToken("test-token", Date.from(Instant.now().plusSeconds(10 * 60)));
    doReturn(accessToken).when(gcpCredentialVendor).vendGcpToken(any());

    credentialOperations =
        new CredentialOperations(awsCredentialVendor, azureCredentialVendor, gcpCredentialVendor);
  }
}
