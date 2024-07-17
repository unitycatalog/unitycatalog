package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.FileUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileUtilsTest {

    @Test
    public void testFileUtils() {

        System.setProperty("storageRoot", "/tmp");

        String tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
        String volumePath = FileUtils.createVolumeDirectory("volume");

        assert tablePath.equals("file:///tmp/catalog/schema/tables/table/");
        assert volumePath.equals("file:///tmp/volume/");

        FileUtils.deleteDirectory(tablePath);
        FileUtils.deleteDirectory(volumePath);

        System.setProperty("storageRoot", "file:///tmp/random");

        tablePath = FileUtils.createTableDirectory("catalog", "schema", "table");
        volumePath = FileUtils.createVolumeDirectory("volume");

        assert tablePath.equals("file:///tmp/random/catalog/schema/tables/table/");
        assert volumePath.equals("file:///tmp/random/volume/");

        FileUtils.deleteDirectory(tablePath);
        FileUtils.deleteDirectory(volumePath);


        assertThrows(BaseException.class, () -> {
            FileUtils.createTableDirectory("..", "schema", "table");
        });

        assertThrows(BaseException.class, () -> {
            FileUtils.createVolumeDirectory("..");
        });


    }
}
