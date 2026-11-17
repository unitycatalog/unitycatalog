package io.unitycatalog.server.sdk.externallocation;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.BaseExternalLocationCRUDTest;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkCredentialOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.Test;

public class SdkExternalLocationCRUDTest extends BaseExternalLocationCRUDTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new SdkVolumeOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected CredentialOperations createCredentialOperations(ServerConfig config) {
    return new SdkCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  /**
   * A path reaches the storage of its location's credential. Vending runs only through the SDK: the
   * CLI tests run the server with the CLI's S3A dependencies, whose bundled AWS SDK differs.
   */
  @Test
  public void testPathsReachTheStorageOfTheirLocationsCredential() throws ApiException {
    String endpointA = "http://storage-a.invalid:9000";
    String endpointB = "http://storage-b.invalid:9000";
    createCompatCredential(COMPAT_CREDENTIAL_NAME, endpointA);
    createCompatCredential(ANOTHER_COMPAT_CREDENTIAL_NAME, endpointB);
    createLocation("el_a", "s3://shared-bucket/a", COMPAT_CREDENTIAL_NAME);
    createLocation("el_b", "s3://shared-bucket/b", ANOTHER_COMPAT_CREDENTIAL_NAME);
    assertThat(vendedS3Endpoint("s3://shared-bucket/a/t")).isEqualTo(endpointA);
    assertThat(vendedS3Endpoint("s3://shared-bucket/b/t")).isEqualTo(endpointB);

    // Updating the credential moves every path under its locations, for example to the storage's
    // new host name.
    String movedEndpoint = "http://storage-a-moved.invalid:9000";
    credentialOperations.updateCredential(
        COMPAT_CREDENTIAL_NAME, new UpdateCredentialRequest().s3Compatible(keyPair(movedEndpoint)));
    assertThat(vendedS3Endpoint("s3://shared-bucket/a/t")).isEqualTo(movedEndpoint);
  }

  /** The S3 endpoint vended for a path; its credential vends no keys, so only the endpoint. */
  private String vendedS3Endpoint(String path) throws ApiException {
    return new TemporaryCredentialsApi(TestUtils.createApiClient(serverConfig))
        .generateTemporaryPathCredentials(
            new GenerateTemporaryPathCredential().url(path).operation(PathOperation.PATH_READ))
        .getAwsTempCredentials()
        .getS3Endpoint();
  }
}
