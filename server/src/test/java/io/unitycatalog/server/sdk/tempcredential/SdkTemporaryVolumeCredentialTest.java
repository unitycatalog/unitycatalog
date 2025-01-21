package io.unitycatalog.server.sdk.tempcredential;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTestWithMockCredentials;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SdkTemporaryVolumeCredentialTest extends BaseCRUDTestWithMockCredentials {
  protected SdkTemporaryCredentialOperations temporaryCredentialOperations;
  protected VolumeOperations volumeOperations;
  protected SchemaOperations schemaOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SdkTemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  protected VolumeOperations createVolumeOperations(ServerConfig serverConfig) {
    return new SdkVolumeOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    temporaryCredentialOperations = createTemporaryCredentialsOperations(serverConfig);
    volumeOperations = createVolumeOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);
  }

  protected void createCatalogAndSchema() throws ApiException {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);

    schemaOperations.createSchema(
        new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
  }

  @ParameterizedTest
  @MethodSource("provideTestArguments")
  public void testGenerateTemporaryCredentialsWhereConfIsProvided(
      String scheme, boolean isConfiguredPath) throws ApiException {
    createCatalogAndSchema();
    String url = getTestCloudPath(scheme, isConfiguredPath);

    URI uri = URI.create(url);
    String volumeName = "testtable-" + uri.getScheme();
    CreateVolumeRequestContent createVolumeRequest =
        new CreateVolumeRequestContent()
            .name(volumeName)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation(url);
    VolumeInfo volumeInfo = volumeOperations.createVolume(createVolumeRequest);

    GenerateTemporaryVolumeCredential generateTemporaryVolumeCredential =
        new GenerateTemporaryVolumeCredential()
            .volumeId(volumeInfo.getVolumeId())
            .operation(VolumeOperation.READ_VOLUME);
    if (isConfiguredPath) {
      TemporaryCredentials temporaryCredentials =
          temporaryCredentialOperations.generateTemporaryVolumeCredentials(
              generateTemporaryVolumeCredential);
      assertTemporaryCredentials(temporaryCredentials, scheme);
    } else {
      assertThatThrownBy(
              () ->
                  temporaryCredentialOperations.generateTemporaryVolumeCredentials(
                      generateTemporaryVolumeCredential))
          .isInstanceOf(ApiException.class);
    }
  }
}
