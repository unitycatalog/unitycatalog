package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

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
  }
}
