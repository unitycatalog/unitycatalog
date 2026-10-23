package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * SDK-based access control tests for Volume CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Volume creation requires ownership of schema
 *   <li>Volume listing is filtered based on permissions
 *   <li>Volume get requires ownership or READ VOLUME permission
 *   <li>Volume update requires ownership
 *   <li>Volume delete requires ownership
 * </ul>
 */
public class SdkVolumeAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  @ParameterizedTest
  @CsvSource({
    "VOLUME, true, true, , true, false",
    "VOLUME, true, true, SCHEMA, true, true",
    "VOLUME, true, true, CATALOG, true, true",
    "VOLUME, false, true, SCHEMA, false, false",
    "VOLUME, true, false, SCHEMA, false, false",
    "SCHEMA, true, false, , true, true",
    "SCHEMA, false, false, , false, false",
    "CATALOG, false, false, , true, false",
    "CATALOG, false, false, SCHEMA, true, false",
    "CATALOG, false, true, SCHEMA, true, true",
    "METASTORE, false, false, , false, false",
    "METASTORE, true, true, SCHEMA, false, false",
    ", true, true, SCHEMA, false, false"
  })
  @SneakyThrows
  public void testVolumeRenamePermissions(
      SecurableType ownerType,
      boolean useCatalog,
      boolean useSchema,
      SecurableType createGrantType,
      boolean canComment,
      boolean canRename) {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();
    SchemasApi schemaCreatorApi =
        new SchemasApi(TestUtils.createApiClient(createTestUserServerConfig(PRINCIPAL_2)));
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        "cat_pr1",
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    schemaCreatorApi.createSchema(new CreateSchema().name("sch_rename").catalogName("cat_pr1"));
    VolumesApi volumeOwnerApi =
        new VolumesApi(TestUtils.createApiClient(createTestUserServerConfig(REGULAR_1)));
    VolumesApi observerApi = new VolumesApi(adminApiClient);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        "cat_pr1.sch_rename",
        Privileges.USE_SCHEMA,
        Privileges.CREATE_VOLUME);
    VolumeInfo original =
        volumeOwnerApi.createVolume(
            new CreateVolumeRequestContent()
                .name("volume")
                .catalogName("cat_pr1")
                .schemaName("sch_rename")
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(testDirectoryRoot.resolve("volume").toString()));
    grantsApi.update(
        SecurableType.CATALOG,
        "cat_pr1",
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(PRINCIPAL_1)
                        .remove(List.of(Privilege.USE_CATALOG)),
                    new PermissionsChange()
                        .principal(PRINCIPAL_2)
                        .remove(List.of(Privilege.USE_CATALOG, Privilege.CREATE_SCHEMA)),
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_CATALOG)))));
    grantsApi.update(
        SecurableType.SCHEMA,
        "cat_pr1.sch_rename",
        new UpdatePermissions()
            .changes(
                List.of(
                    new PermissionsChange()
                        .principal(REGULAR_1)
                        .remove(List.of(Privilege.USE_SCHEMA, Privilege.CREATE_VOLUME)))));
    String fullName = "cat_pr1.sch_rename.volume";
    String principal =
        ownerType == null
            ? REGULAR_2
            : switch (ownerType) {
              case METASTORE -> "admin";
              case CATALOG -> PRINCIPAL_1;
              case SCHEMA -> PRINCIPAL_2;
              default -> REGULAR_1;
            };
    VolumesApi callerApi =
        ownerType == SecurableType.METASTORE
            ? new VolumesApi(adminApiClient)
            : new VolumesApi(TestUtils.createApiClient(createTestUserServerConfig(principal)));
    if (useCatalog) {
      grantPermissions(principal, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    }
    if (useSchema) {
      grantPermissions(
          principal, SecurableType.SCHEMA, "cat_pr1.sch_rename", Privileges.USE_SCHEMA);
    }
    if (createGrantType != null) {
      grantPermissions(
          principal,
          createGrantType,
          createGrantType == SecurableType.CATALOG ? "cat_pr1" : "cat_pr1.sch_rename",
          Privileges.CREATE_VOLUME);
    }

    UpdateVolumeRequestContent commentOnly = new UpdateVolumeRequestContent().comment("comment");
    if (canComment) {
      callerApi.updateVolume(fullName, commentOnly);
      assertThat(observerApi.getVolume(fullName).getComment()).isEqualTo("comment");
    } else {
      assertPermissionDenied(() -> callerApi.updateVolume(fullName, commentOnly));
      assertThat(observerApi.getVolume(fullName).getComment()).isEqualTo(original.getComment());
    }

    UpdateVolumeRequestContent sameName = new UpdateVolumeRequestContent().newName("volume");
    if (canRename) {
      assertApiException(
          () -> callerApi.updateVolume(fullName, sameName),
          ErrorCode.RESOURCE_ALREADY_EXISTS,
          "Volume already exists");
    } else {
      assertPermissionDenied(() -> callerApi.updateVolume(fullName, sameName));
    }

    for (String comment : new String[] {null, "renamed comment"}) {
      UpdateVolumeRequestContent rename =
          new UpdateVolumeRequestContent().newName("renamed").comment(comment);
      if (canRename) {
        callerApi.updateVolume(fullName, rename);
        VolumeInfo renamed = observerApi.getVolume("cat_pr1.sch_rename.renamed");
        assertThat(renamed.getVolumeId()).isEqualTo(original.getVolumeId());
        assertThat(renamed.getName()).isEqualTo("renamed");
        assertThat(renamed.getComment()).isEqualTo(comment == null ? "comment" : comment);
        callerApi.updateVolume(
            "cat_pr1.sch_rename.renamed", new UpdateVolumeRequestContent().newName("volume"));
      } else {
        assertPermissionDenied(() -> callerApi.updateVolume(fullName, rename));
        VolumeInfo unchanged = observerApi.getVolume(fullName);
        assertThat(unchanged.getName()).isEqualTo("volume");
        assertThat(unchanged.getComment())
            .isEqualTo(canComment ? "comment" : original.getComment());
      }
    }
  }

  @Test
  @SneakyThrows
  public void testVolumeAccess() {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();

    // Create API clients for different users
    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig regular1Config = createTestUserServerConfig(REGULAR_1);
    ServerConfig regular2Config = createTestUserServerConfig("regular-2@localhost");

    SchemasApi regular2SchemasApi = new SchemasApi(TestUtils.createApiClient(regular2Config));
    VolumesApi adminVolumesApi = new VolumesApi(adminApiClient);
    VolumesApi principal1VolumesApi = new VolumesApi(TestUtils.createApiClient(principal1Config));
    VolumesApi regular1VolumesApi = new VolumesApi(TestUtils.createApiClient(regular1Config));

    // grant USE CATALOG and USE SCHEMA to principal-1
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // grant USE CATALOG to regular-1
    grantPermissions(REGULAR_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);

    // grant CREATE SCHEMA and USE CATALOG to regular-2
    grantPermissions(
        "regular-2@localhost", SecurableType.CATALOG, "cat_pr1", Privileges.CREATE_SCHEMA);
    grantPermissions(
        "regular-2@localhost", SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);

    // create schema as regular-2
    CreateSchema createSchema2 = new CreateSchema().name("sch_rg2").catalogName("cat_pr1");
    SchemaInfo schema2Info = regular2SchemasApi.createSchema(createSchema2);
    assertThat(schema2Info).isNotNull();

    // grant USE SCHEMA to regular-2
    grantPermissions(
        "regular-2@localhost", SecurableType.SCHEMA, "cat_pr1.sch_rg2", Privileges.USE_SCHEMA);

    // create volume (admin) -> metastore admin, no schema ownership -> denied
    CreateVolumeRequestContent createVolumeAdmin =
        new CreateVolumeRequestContent()
            .name("vol_adm")
            .catalogName("cat_pr1")
            .schemaName("sch_rg2")
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("/tmp/vol_adm");
    assertPermissionDenied(() -> adminVolumesApi.createVolume(createVolumeAdmin));

    // create volume (principal-1) -> catalog owner, schema owner -> allowed
    CreateVolumeRequestContent createVolume1 =
        new CreateVolumeRequestContent()
            .name("vol_adm")
            .catalogName("cat_pr1")
            .schemaName("sch_pr1")
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("/tmp/volume3");
    VolumeInfo volume1Info = principal1VolumesApi.createVolume(createVolume1);
    assertThat(volume1Info).isNotNull();
    assertThat(volume1Info.getName()).isEqualTo("vol_adm");

    // create volume (principal-1) -> catalog owner, schema owner -> allowed
    CreateVolumeRequestContent createVolume2 =
        new CreateVolumeRequestContent()
            .name("vol_adm2")
            .catalogName("cat_pr1")
            .schemaName("sch_rg2")
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("/tmp/volume3");
    assertPermissionDenied(() -> principal1VolumesApi.createVolume(createVolume2));

    // list volumes (admin) -> metastore admin -> allowed - list all
    List<VolumeInfo> adminVolumes = listAllVolumes(adminVolumesApi, "cat_pr1", "sch_pr1");
    assertThat(adminVolumes).hasSize(1);

    // list volumes (principal-1) -> owner -> allowed - list all
    List<VolumeInfo> principal1Volumes = listAllVolumes(principal1VolumesApi, "cat_pr1", "sch_pr1");
    assertThat(principal1Volumes).hasSize(1);

    // get volume (principal-1) -> owner -> allowed
    VolumeInfo volumeInfoOwner = principal1VolumesApi.getVolume("cat_pr1.sch_pr1.vol_adm");
    assertThat(volumeInfoOwner).isNotNull();

    // get volume (admin) -> metastore admin -> allowed
    VolumeInfo volumeInfoAdmin = adminVolumesApi.getVolume("cat_pr1.sch_pr1.vol_adm");
    assertThat(volumeInfoAdmin).isNotNull();

    // get volume (regular-1) -> no permission -> denied
    assertPermissionDenied(() -> regular1VolumesApi.getVolume("cat_pr1.sch_pr1.vol_adm"));

    // update volume (principal-1) -> catalog owner, schema owner, USE SCHEMA -> allowed
    VolumeInfo updatedVolume =
        principal1VolumesApi.updateVolume(
            "cat_pr1.sch_pr1.vol_adm",
            new UpdateVolumeRequestContent().comment("principal update"));
    assertThat(updatedVolume.getComment()).isEqualTo("principal update");

    // update volume (regular-1) -> not owner [catalog] -> denied
    assertPermissionDenied(
        () ->
            regular1VolumesApi.updateVolume(
                "cat_pr1.sch_pr1.vol_adm",
                new UpdateVolumeRequestContent().comment("regular update")));

    // delete volume (regular-1) -> not owner -> denied
    assertPermissionDenied(() -> regular1VolumesApi.deleteVolume("cat_pr1.sch_pr1.vol_adm"));

    // delete volume (principal-1) -> owner -> allowed
    principal1VolumesApi.deleteVolume("cat_pr1.sch_pr1.vol_adm");

    // A catalog/schema owner can update a volume it does not own: updateVolume mirrors its sibling
    // deleteVolume, letting a catalog/schema owner govern the child volume. regular-1 owns the
    // volume; principal-1 owns only the catalog and schema.
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.CREATE_VOLUME);
    VolumeInfo volRg1 =
        regular1VolumesApi.createVolume(
            new CreateVolumeRequestContent()
                .name("vol_rg1")
                .catalogName("cat_pr1")
                .schemaName("sch_pr1")
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation("/tmp/vol_rg1"));
    assertThat(volRg1.getName()).isEqualTo("vol_rg1");

    VolumeInfo updatedByOwner =
        principal1VolumesApi.updateVolume(
            "cat_pr1.sch_pr1.vol_rg1", new UpdateVolumeRequestContent().comment("owner-cascade"));
    assertThat(updatedByOwner.getComment()).isEqualTo("owner-cascade");
  }
}
