package io.unitycatalog.server.sdk.externallocation;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.UpdateCredentialRequest;
import io.unitycatalog.client.model.UpdateExternalLocation;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.credential.CredentialOperations;
import io.unitycatalog.server.base.externallocation.BaseExternalLocationCRUDTest;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkCredentialOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SdkExternalLocationCRUDTest extends BaseExternalLocationCRUDTest {
  @Test
  public void testExternalLocationWritesPreservePercentKeys() throws ApiException {
    for (String scheme : List.of("s3", "gs")) {
      String root = scheme + "://bucket/data%20lake/value=%2F%25";
      String name = "percent_" + scheme;
      create(name, root);
      externalLocationOperations.updateExternalLocation(
          name, new UpdateExternalLocation().url(root + "/child"));
      assertThat(externalLocationOperations.getExternalLocation(name).getUrl())
          .isEqualTo(root + "/child");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket/data",
        "gs://bucket/data",
        "abfs://c@a.dfs.core.windows.net/data",
        "abfss://c@a.dfs.core.windows.net/data"
      })
  public void testExternalLocationWritesRejectInvalidCloudRoots(String root) throws ApiException {
    String name = "cloud_root";
    create(name, root);
    List<String> suffixes = new ArrayList<>(List.of("?query=1", "#fragment"));
    if (root.startsWith("abfs")) {
      suffixes.add("%20lake");
    }
    for (String suffix : suffixes) {
      String invalidRoot = root + suffix;
      String message =
          suffix.startsWith("%")
              ? "must not contain percent escapes"
              : "must not contain a query or a fragment";
      TestUtils.assertApiException(
          () ->
              externalLocationOperations.createExternalLocation(
                  new CreateExternalLocation()
                      .name("invalid_root")
                      .url(invalidRoot)
                      .credentialName(credentialInfo.getName())),
          ErrorCode.INVALID_ARGUMENT,
          message);
      TestUtils.assertApiException(
          () ->
              externalLocationOperations.updateExternalLocation(
                  name, new UpdateExternalLocation().url(invalidRoot)),
          ErrorCode.INVALID_ARGUMENT,
          message);
      assertThat(externalLocationOperations.getExternalLocation(name).getUrl()).isEqualTo(root);
    }
    TestUtils.assertApiException(
        () -> externalLocationOperations.getExternalLocation("invalid_root"),
        ErrorCode.NOT_FOUND,
        "External location not found");
  }

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
