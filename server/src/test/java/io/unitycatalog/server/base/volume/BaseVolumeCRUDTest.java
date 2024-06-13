package io.unitycatalog.server.base.volume;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.persist.FileUtils;
import io.unitycatalog.server.persist.HibernateUtil;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.*;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;

import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;
import static io.unitycatalog.server.utils.TestUtils.*;

public abstract class BaseVolumeCRUDTest extends BaseCRUDTest {
    protected SchemaOperations schemaOperations;
    protected VolumeOperations volumeOperations;
    @Before
    public void setUp() {
        super.setUp();
        schemaOperations = createSchemaOperations(serverConfig);
        volumeOperations = createVolumeOperations(serverConfig);
        cleanUp();
        //createCommonResources();
    }

    protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

    protected abstract VolumeOperations createVolumeOperations(ServerConfig serverConfig);

    protected void cleanUp() {
        try {
            if (volumeOperations.getVolume(VOLUME_FULL_NAME) != null) {
                volumeOperations.deleteVolume(VOLUME_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (volumeOperations.getVolume(VOLUME_NEW_FULL_NAME) != null) {
                volumeOperations.deleteVolume(VOLUME_NEW_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(SCHEMA_FULL_NAME) != null) {
                schemaOperations.deleteSchema(SCHEMA_FULL_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        super.cleanUp();
    }

    protected void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog and schema
        catalogOperations.createCatalog(CATALOG_NAME, "Common catalog for volumes");
        schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    }

    @Test
    public void testVolumeCRUD() throws ApiException {
        // Create a volume
        System.out.println("Testing create volume..");
        CreateVolumeRequestContent createVolumeRequest = new CreateVolumeRequestContent()
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
        assertEquals(FileUtils.convertRelativePathToURI(createVolumeRequest.getStorageLocation()), volumeInfo.getStorageLocation());
        assertEquals(VOLUME_FULL_NAME, volumeInfo.getFullName());
        assertNotNull(volumeInfo.getCreatedAt());

        // List volumes
        System.out.println("Testing list volumes..");
        Iterable<VolumeInfo> volumeInfos = volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME);
        assertTrue(contains(volumeInfos, volumeInfo, (volume) -> {
            assert volume.getName() != null;
            return volume.getName().equals(VOLUME_NAME);
        }));

        // Get volume
        System.out.println("Testing get volume..");
        VolumeInfo retrievedVolumeInfo = volumeOperations.getVolume(VOLUME_FULL_NAME);
        assertEquals(volumeInfo, retrievedVolumeInfo);

        // Update volume
        System.out.println("Testing update volume..");
        UpdateVolumeRequestContent updateVolumeRequest = new UpdateVolumeRequestContent()
                .newName(VOLUME_NEW_NAME)
                .comment(COMMENT);
        // Set update details
        VolumeInfo updatedVolumeInfo = volumeOperations.updateVolume(VOLUME_FULL_NAME, updateVolumeRequest);
        assertEquals(updateVolumeRequest.getNewName(), updatedVolumeInfo.getName());
        assertEquals(updateVolumeRequest.getComment(), updatedVolumeInfo.getComment());
        assertEquals(VOLUME_NEW_FULL_NAME, updatedVolumeInfo.getFullName());
        assertNotNull(updatedVolumeInfo.getUpdatedAt());

        // Delete volume
        System.out.println("Testing delete volume..");
        volumeOperations.deleteVolume(VOLUME_NEW_FULL_NAME);
        assertEquals(0, getSize(volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME)));


        // Testing Managed Volume
        System.out.println("Creating managed volume..");

        SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

        try (Session session = sessionFactory.openSession()) {
            session.beginTransaction();
            VolumeInfoDAO managedVolume = VolumeInfoDAO.builder()
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .volumeType(VolumeType.MANAGED.getValue())
                    .storageLocation("/tmp/managed_volume")
                    .name(VOLUME_NAME)
                    .fullName(VOLUME_FULL_NAME)
                    .createdAt(new Date())
                    .updatedAt(new Date())
                    .volumeId(UUID.randomUUID().toString())
                    .build();
            session.persist(managedVolume);
            session.getTransaction().commit();
        }

        VolumeInfo managedVolumeInfo = volumeOperations.getVolume(VOLUME_FULL_NAME);
        assertEquals(VolumeType.MANAGED, managedVolumeInfo.getVolumeType());
        assertEquals(FileUtils.convertRelativePathToURI("/tmp/managed_volume"), managedVolumeInfo.getStorageLocation());
        assertEquals(VOLUME_FULL_NAME, managedVolumeInfo.getFullName());
        assertEquals(VOLUME_NAME, managedVolumeInfo.getName());
        assertEquals(CATALOG_NAME, managedVolumeInfo.getCatalogName());
        assertEquals(SCHEMA_NAME, managedVolumeInfo.getSchemaName());
        assertNotNull(managedVolumeInfo.getCreatedAt());
        assertNotNull(managedVolumeInfo.getUpdatedAt());

        // List volumes
        System.out.println("Testing list managed volumes..");
        Iterable<VolumeInfo> volumeInfosManaged = volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME);
        assertEquals(1, getSize(volumeInfosManaged));
        assertTrue(contains(volumeInfosManaged, managedVolumeInfo, (volume) -> {
            assert volume.getName() != null;
            return volume.getName().equals(VOLUME_NAME);
        }));

        // Delete volume
        System.out.println("Testing delete volume..");
        volumeOperations.deleteVolume(VOLUME_FULL_NAME);
        assertEquals(0, getSize(volumeOperations.listVolumes(CATALOG_NAME, SCHEMA_NAME)));
    }
}