package io.unitycatalog.server.base.volume;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.FileOperations;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseVolumeCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected VolumeOperations volumeOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    volumeOperations = createVolumeOperations(serverConfig);
  }

  private SchemaInfo schemaInfo;

  protected void createCommonResources() throws ApiException {
    // Common setup operations such as creating a catalog and schema
    CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaInfo =
        schemaOperations.createSchema(
            new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  protected void assertVolume(
      VolumeInfo volumeInfo,
      CreateVolumeRequestContent createVolumeRequest,
      String volumeFullName) {
    assertThat(volumeInfo.getName()).isEqualTo(createVolumeRequest.getName());
    assertThat(volumeInfo.getCatalogName()).isEqualTo(createVolumeRequest.getCatalogName());
    assertThat(volumeInfo.getSchemaName()).isEqualTo(createVolumeRequest.getSchemaName());
    assertThat(volumeInfo.getVolumeType()).isEqualTo(createVolumeRequest.getVolumeType());
    assertThat(volumeInfo.getStorageLocation())
        .isEqualTo(
            FileOperations.convertRelativePathToURI(createVolumeRequest.getStorageLocation()));
    assertThat(volumeInfo.getFullName()).isEqualTo(volumeFullName);
    assertThat(volumeInfo.getCreatedAt()).isNotNull();
  }

  @Test
  public void testVolumeCRUD() throws ApiException {
    // Create a volume
    System.out.println("Testing create volume..");
    CreateVolumeRequestContent createVolumeRequest =
        new CreateVolumeRequestContent()
            .name(VOLUME_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("/tmp/volume1");
    assertThatThrownBy(() -> volumeOperations.createVolume(createVolumeRequest))
        .isInstanceOf(Exception.class);

    createCommonResources();
    VolumeInfo volumeInfo = volumeOperations.createVolume(createVolumeRequest);
    assertVolume(volumeInfo, createVolumeRequest, VOLUME_FULL_NAME);

    // Create another volume to test pagination
    CreateVolumeRequestContent createVolumeRequest2 =
        new CreateVolumeRequestContent()
            .name(COMMON_ENTITY_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .volumeType(VolumeType.EXTERNAL)
            .storageLocation("/tmp/volume2");
    VolumeInfo volumeInfo2 = volumeOperations.createVolume(createVolumeRequest2);
    assertVolume(
        volumeInfo2,
        createVolumeRequest2,
        CATALOG_NAME + '.' + SCHEMA_NAME + '.' + COMMON_ENTITY_NAME);

    // List volumes
    System.out.println("Testing list volumes..");
    Iterable<VolumeInfo> volumeInfos =
        volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.empty());
    assertThat(volumeInfos).contains(volumeInfo);

    // List volumes with page token
    System.out.println("Testing list volumes with page token..");
    volumeInfos = volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.of(VOLUME_NAME));
    assertThat(volumeInfos).doesNotContain(volumeInfo);
    assertThat(volumeInfos).contains(volumeInfo2);

    // Get volume
    System.out.println("Testing get volume..");
    VolumeInfo retrievedVolumeInfo = volumeOperations.getVolume(VOLUME_FULL_NAME);
    assertThat(retrievedVolumeInfo).isEqualTo(volumeInfo);

    // Calling update volume with nothing to update should not change anything
    System.out.println("Testing updating volume with nothing to update..");
    UpdateVolumeRequestContent emptyUpdateVolumeRequest = new UpdateVolumeRequestContent();
    volumeOperations.updateVolume(VOLUME_FULL_NAME, emptyUpdateVolumeRequest);
    VolumeInfo retrievedVolumeInfo2 = volumeOperations.getVolume(VOLUME_FULL_NAME);
    assertThat(retrievedVolumeInfo2).isEqualTo(volumeInfo);

    // Update volume name without updating comment
    System.out.println("Testing update volume: changing name..");
    UpdateVolumeRequestContent updateVolumeRequest =
        new UpdateVolumeRequestContent().newName(VOLUME_NEW_NAME);
    VolumeInfo updatedVolumeInfo =
        volumeOperations.updateVolume(VOLUME_FULL_NAME, updateVolumeRequest);
    assertThat(updatedVolumeInfo.getName()).isEqualTo(updateVolumeRequest.getNewName());
    assertThat(updatedVolumeInfo.getComment()).isEqualTo(updateVolumeRequest.getComment());
    assertThat(updatedVolumeInfo.getFullName()).isEqualTo(VOLUME_NEW_FULL_NAME);
    assertThat(updatedVolumeInfo.getUpdatedAt()).isNotNull();

    // Update volume comment without updating name
    System.out.println("Testing update volume: changing comment..");
    UpdateVolumeRequestContent updateVolumeRequest2 =
        new UpdateVolumeRequestContent().comment(COMMENT);
    VolumeInfo updatedVolumeInfo2 =
        volumeOperations.updateVolume(VOLUME_NEW_FULL_NAME, updateVolumeRequest2);
    assertThat(updatedVolumeInfo2.getName()).isEqualTo(VOLUME_NEW_NAME);
    assertThat(updatedVolumeInfo2.getComment()).isEqualTo(updateVolumeRequest2.getComment());
    assertThat(updatedVolumeInfo2.getFullName()).isEqualTo(VOLUME_NEW_FULL_NAME);
    assertThat(updatedVolumeInfo2.getUpdatedAt()).isNotNull();

    // Delete volume
    System.out.println("Testing delete volume..");
    volumeOperations.deleteVolume(VOLUME_NEW_FULL_NAME);
    assertThat(volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.empty()))
        .doesNotContain(volumeInfo);

    // Testing Managed Volume
    System.out.println("Creating managed volume..");

    SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();

    try (Session session = sessionFactory.openSession()) {
      session.beginTransaction();
      VolumeInfoDAO managedVolume =
          VolumeInfoDAO.builder()
              .volumeType(VolumeType.MANAGED.getValue())
              .storageLocation("/tmp/managed_volume")
              .name(VOLUME_NAME)
              .createdAt(new Date())
              .updatedAt(new Date())
              .id(UUID.randomUUID())
              .schemaId(UUID.fromString(schemaInfo.getSchemaId()))
              .build();
      session.persist(managedVolume);
      session.getTransaction().commit();
    }

    VolumeInfo managedVolumeInfo = volumeOperations.getVolume(VOLUME_FULL_NAME);
    assertThat(managedVolumeInfo.getVolumeType()).isEqualTo(VolumeType.MANAGED);
    assertThat(managedVolumeInfo.getStorageLocation())
        .isEqualTo(FileOperations.convertRelativePathToURI("/tmp/managed_volume"));
    assertThat(managedVolumeInfo.getFullName()).isEqualTo(VOLUME_FULL_NAME);
    assertThat(managedVolumeInfo.getName()).isEqualTo(VOLUME_NAME);
    assertThat(managedVolumeInfo.getCatalogName()).isEqualTo(CATALOG_NAME);
    assertThat(managedVolumeInfo.getSchemaName()).isEqualTo(SCHEMA_NAME);
    assertThat(managedVolumeInfo.getCreatedAt()).isNotNull();
    assertThat(managedVolumeInfo.getUpdatedAt()).isNotNull();

    // List volumes
    System.out.println("Testing list managed volumes..");
    Iterable<VolumeInfo> volumeInfosManaged =
        volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.empty());
    assertThat(volumeInfosManaged).hasSize(2).contains(managedVolumeInfo);

    // List volumes with page token
    System.out.println("Testing list managed volumes with page token..");
    Iterable<VolumeInfo> volumeInfosManaged2 =
        volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.of(VOLUME_NAME));
    assertThat(volumeInfosManaged2).hasSize(1).contains(volumeInfo2);

    // Delete the volume created to test pagination
    volumeOperations.deleteVolume(CATALOG_NAME + '.' + SCHEMA_NAME + '.' + COMMON_ENTITY_NAME);
    assertThat(volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME, Optional.empty()))
        .doesNotContain(volumeInfo2);

    // NOW Update the schema name
    schemaOperations.updateSchema(
        SCHEMA_FULL_NAME, new UpdateSchema().newName(SCHEMA_NEW_NAME).comment(SCHEMA_NEW_COMMENT));
    // get volume
    VolumeInfo volumePostSchemaNameChange =
        volumeOperations.getVolume(CATALOG_NAME + "." + SCHEMA_NEW_NAME + "." + VOLUME_NAME);
    assertThat(managedVolumeInfo.getVolumeId()).isEqualTo(volumePostSchemaNameChange.getVolumeId());

    // test delete parent schema when volume exists
    assertThatThrownBy(
            () ->
                schemaOperations.deleteSchema(
                    CATALOG_NAME + "." + SCHEMA_NEW_NAME, Optional.of(false)))
        .isInstanceOf(Exception.class);

    // test force delete parent schema when volume exists
    schemaOperations.deleteSchema(CATALOG_NAME + "." + SCHEMA_NEW_NAME, Optional.of(true));
    // both schema and volume should be deleted
    assertThatThrownBy(
            () ->
                volumeOperations.getVolume(
                    CATALOG_NAME + "." + SCHEMA_NEW_NAME + "." + VOLUME_NAME))
        .isInstanceOf(Exception.class);
    assertThatThrownBy(() -> schemaOperations.getSchema(CATALOG_NAME + "." + SCHEMA_NEW_NAME))
        .isInstanceOf(Exception.class);
  }
}
