package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SdkVolumeRenameLegacyAccessControlTest extends SdkAccessControlBaseCRUDTest {

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STRICT_PRIVILEGE_CHECKS_ENABLED.getKey(), "false");
  }

  @ParameterizedTest
  @CsvSource({
    "true, true, true, true",
    "true, false, true, false",
    "true, true, false, false",
    "false, true, true, false"
  })
  @SneakyThrows
  public void testLegacyRenamePermissions(
      boolean owner, boolean useCatalog, boolean useSchema, boolean canRename) {
    createTestUser(REGULAR_1);
    createTestUser(REGULAR_2);
    VolumesApi ownerApi =
        new VolumesApi(TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1)));
    VolumesApi observerApi = new VolumesApi(adminApiClient);
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        TestUtils.SCHEMA_FULL_NAME,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_VOLUME);
    VolumeInfo original =
        ownerApi.createVolume(
            new CreateVolumeRequestContent()
                .name("volume")
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(testDirectoryRoot.resolve("volume").toString()));
    grantsApi.update(
        SecurableType.CATALOG,
        TestUtils.CATALOG_NAME,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_CATALOG)))));
    grantsApi.update(
        SecurableType.SCHEMA,
        TestUtils.SCHEMA_FULL_NAME,
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_SCHEMA, Privilege.CREATE_VOLUME)))));
    String principal = owner ? REGULAR_1 : REGULAR_2;
    if (useCatalog) {
      grantPermissions(
          principal, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    }
    if (useSchema) {
      grantPermissions(
          principal, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    }
    VolumesApi callerApi =
        new VolumesApi(TestUtils.createApiClient(createTestUserServerConfig(principal)));
    String fullName = TestUtils.SCHEMA_FULL_NAME + ".volume";
    UpdateVolumeRequestContent rename =
        new UpdateVolumeRequestContent().newName("renamed").comment("renamed comment");
    if (canRename) {
      assertApiException(
          () ->
              callerApi.updateVolume(fullName, new UpdateVolumeRequestContent().newName("volume")),
          ErrorCode.RESOURCE_ALREADY_EXISTS,
          "Volume already exists");
      callerApi.updateVolume(fullName, rename);
      VolumeInfo renamed =
          observerApi.getVolume(
              TestUtils.SCHEMA_FULL_NAME + ".renamed", /* includeBrowse= */ false);
      assertThat(renamed.getVolumeId()).isEqualTo(original.getVolumeId());
      assertThat(renamed.getName()).isEqualTo("renamed");
      assertThat(renamed.getComment()).isEqualTo("renamed comment");
    } else {
      assertPermissionDenied(() -> callerApi.updateVolume(fullName, rename));
      VolumeInfo unchanged = observerApi.getVolume(fullName, /* includeBrowse= */ false);
      assertThat(unchanged.getName()).isEqualTo("volume");
      assertThat(unchanged.getComment()).isEqualTo(original.getComment());
    }
  }
}
