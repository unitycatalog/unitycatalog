package io.unitycatalog.server.sdk.volume;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.volume.BaseVolumeCRUDTest;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkVolumeCRUDTest extends BaseVolumeCRUDTest {

    @BeforeClass
    public static void setUpClass() {
        // Any static setup specific to this test class
    }

    @AfterClass
    public static void tearDownClass() {
        // Any static teardown specific to this test class
    }

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
}