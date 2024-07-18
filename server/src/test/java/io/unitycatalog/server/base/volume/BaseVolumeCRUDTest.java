package io.unitycatalog.server.base.volume;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.*;

public abstract class BaseVolumeCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected VolumeOperations volumeOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

  @Before
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
    assertThrows(Exception.class, () -> volumeOperations.createVolume(createVolumeRequest));

    createCommonResources();
    VolumeInfo volumeInfo = volumeOperations.createVolume(createVolumeRequest);
    assertEquals(createVolumeRequest.getName(), volumeInfo.getName());
    assertEquals(createVolumeRequest.getCatalogName(), volumeInfo.getCatalogName());
    assertEquals(createVolumeRequest.getSchemaName(), volumeInfo.getSchemaName());
    assertEquals(createVolumeRequest.getVolumeType(), volumeInfo.getVolumeType());
    assertEquals(
        FileUtils.convertRelativePathToURI(createVolumeRequest.getStorageLocation()),
        volumeInfo.getStorageLocation());
    assertEquals(VOLUME_FULL_NAME, volumeInfo.getFullName());
    assertNotNull(volumeInfo.getCreatedAt());

    // List volumes
    System.out.println("Testing list volumes..");
    Iterable<VolumeInfo> volumeInfos = volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME);
    assertTrue(
        contains(
            volumeInfos,
            volumeInfo,
            (volume) -> {
              assertThat(volume.getName()).isNotNull();
              return volume.getName().equals(VOLUME_NAME);
            }));

    // Get volume
    System.out.println("Testing get volume..");
    VolumeInfo retrievedVolumeInfo = volumeOperations.getVolume(VOLUME_FULL_NAME);
    assertEquals(volumeInfo, retrievedVolumeInfo);

    // Calling update volume with nothing to update should not change anything
    System.out.println("Testing updating volume with nothing to update..");
    UpdateVolumeRequestContent emptyUpdateVolumeRequest = new UpdateVolumeRequestContent();
    VolumeInfo emptyUpdatedVolumeInfo =
        volumeOperations.updateVolume(VOLUME_FULL_NAME, emptyUpdateVolumeRequest);
    VolumeInfo retrievedVolumeInfo2 = volumeOperations.getVolume(VOLUME_FULL_NAME);
    assertEquals(volumeInfo, retrievedVolumeInfo2);

    // Update volume name without updating comment
    System.out.println("Testing update volume: changing name..");
    UpdateVolumeRequestContent updateVolumeRequest =
        new UpdateVolumeRequestContent().newName(VOLUME_NEW_NAME);
    VolumeInfo updatedVolumeInfo =
        volumeOperations.updateVolume(VOLUME_FULL_NAME, updateVolumeRequest);
    assertEquals(updateVolumeRequest.getNewName(), updatedVolumeInfo.getName());
    assertEquals(updateVolumeRequest.getComment(), updatedVolumeInfo.getComment());
    assertEquals(VOLUME_NEW_FULL_NAME, updatedVolumeInfo.getFullName());
    assertNotNull(updatedVolumeInfo.getUpdatedAt());

    // Update volume comment without updating name
    System.out.println("Testing update volume: changing comment..");
    UpdateVolumeRequestContent updateVolumeRequest2 =
        new UpdateVolumeRequestContent().comment(COMMENT);
    VolumeInfo updatedVolumeInfo2 =
        volumeOperations.updateVolume(VOLUME_NEW_FULL_NAME, updateVolumeRequest2);
    assertEquals(VOLUME_NEW_NAME, updatedVolumeInfo2.getName());
    assertEquals(updateVolumeRequest2.getComment(), updatedVolumeInfo2.getComment());
    assertEquals(VOLUME_NEW_FULL_NAME, updatedVolumeInfo2.getFullName());
    assertNotNull(updatedVolumeInfo2.getUpdatedAt());

    // Delete volume
    System.out.println("Testing delete volume..");
    volumeOperations.deleteVolume(VOLUME_NEW_FULL_NAME);
    assertEquals(0, getSize(volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME)));

    // Testing Managed Volume
    System.out.println("Creating managed volume..");

    SessionFactory sessionFactory = HibernateUtils.getSessionFactory();

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
    assertEquals(VolumeType.MANAGED, managedVolumeInfo.getVolumeType());
    assertEquals(
        FileUtils.convertRelativePathToURI("/tmp/managed_volume"),
        managedVolumeInfo.getStorageLocation());
    assertEquals(VOLUME_FULL_NAME, managedVolumeInfo.getFullName());
    assertEquals(VOLUME_NAME, managedVolumeInfo.getName());
    assertEquals(CATALOG_NAME, managedVolumeInfo.getCatalogName());
    assertEquals(SCHEMA_NAME, managedVolumeInfo.getSchemaName());
    assertNotNull(managedVolumeInfo.getCreatedAt());
    assertNotNull(managedVolumeInfo.getUpdatedAt());

    // List volumes
    System.out.println("Testing list managed volumes..");
    Iterable<VolumeInfo> volumeInfosManaged =
        volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME);
    assertEquals(1, getSize(volumeInfosManaged));
    assertTrue(
        contains(
            volumeInfosManaged,
            managedVolumeInfo,
            (volume) -> {
              assertThat(volume.getName()).isNotNull();
              return volume.getName().equals(VOLUME_NAME);
            }));

    // NOW Update the schema name
    schemaOperations.updateSchema(
        SCHEMA_FULL_NAME, new UpdateSchema().newName(SCHEMA_NEW_NAME).comment(SCHEMA_COMMENT));
    // get volume
    VolumeInfo volumePostSchemaNameChange =
        volumeOperations.getVolume(CATALOG_NAME + "." + SCHEMA_NEW_NAME + "." + VOLUME_NAME);
    assertEquals(volumePostSchemaNameChange.getVolumeId(), managedVolumeInfo.getVolumeId());

    // test delete parent schema when volume exists
    assertThrows(
        Exception.class,
        () ->
            schemaOperations.deleteSchema(
                CATALOG_NAME + "." + SCHEMA_NEW_NAME, Optional.of(false)));

    // test force delete parent schema when volume exists
    schemaOperations.deleteSchema(CATALOG_NAME + "." + SCHEMA_NEW_NAME, Optional.of(true));
    // both schema and volume should be deleted
    assertThrows(
        Exception.class,
        () -> volumeOperations.getVolume(CATALOG_NAME + "." + SCHEMA_NEW_NAME + "." + VOLUME_NAME));
    assertThrows(
        Exception.class, () -> schemaOperations.getSchema(CATALOG_NAME + "." + SCHEMA_NEW_NAME));
  }
}
