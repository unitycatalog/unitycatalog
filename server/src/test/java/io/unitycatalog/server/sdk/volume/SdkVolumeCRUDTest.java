package io.unitycatalog.server.sdk.volume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.volume.BaseVolumeCRUDTest;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.jupiter.api.Test;

public class SdkVolumeCRUDTest extends BaseVolumeCRUDTest {

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new SdkVolumeOperations(TestUtils.createApiClient(config));
  }

  @Test
  public void testCreateExternalVolumeRejectsCloudStorageRoot() throws ApiException {
    createCommonResources();
    CreateVolumeRequestContent request =
        new CreateVolumeRequestContent()
            .name("root_location_volume")
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("s3://bucket/");

    assertThatExceptionOfType(ApiException.class)
        .isThrownBy(() -> volumeOperations.createVolume(request))
        .satisfies(
            exception ->
                assertThat(exception.getCode())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT.getHttpStatus().code()))
        .withMessageContaining("must include a non-empty path prefix")
        .withMessageContaining("s3://bucket/");
  }
}
