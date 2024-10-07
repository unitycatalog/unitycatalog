package io.unitycatalog.integrationtests;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class BaseSparkTest {
    // todo: parameterize such that catalogs can be specified per cloud provider if desired
    private static final String ServerUrl = System.getenv().getOrDefault("CATALOG_URI", "http://localhost:8080");
    private static final String AuthToken = System.getenv().getOrDefault("CATALOG_AUTH_TOKEN", "");
    private static final String CatalogName = System.getenv().getOrDefault("CATALOG_NAME", "unity");
    protected static SparkSession spark;

    @BeforeAll
    public static void setup() {
        spark = createSparkSessionWithCatalogs(CatalogName);
    }

    protected static SparkSession createSparkSessionWithCatalogs(String... catalogs) {
        SparkSession.Builder builder =
                SparkSession.builder()
                        .appName("test")
                        .master("local[*]")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog")
                        // s3 conf
                        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                        // GCS conf
                        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

        for (String catalog : catalogs) {
            String catalogConf = "spark.sql.catalog." + catalog;
            builder =
                    builder
                            .config(catalogConf, "io.unitycatalog.spark.UCSingleCatalog")
                            .config(catalogConf + ".uri", ServerUrl)
                            .config(catalogConf + ".token", AuthToken);
        }
        if (catalogs.length > 0) {
            builder.config("spark.sql.defaultCatalog", catalogs[0]);
        }
        return builder.getOrCreate();
    }

    protected static String getBaseLocation(LocationType locationType) throws IOException {
        return switch (locationType) {
            // todo: add hook to clean up temp directory
            case FILE -> Files.createTempDirectory("uc-test-table").toFile().getAbsolutePath();
            case S3 -> System.getenv("S3_BASE_LOCATION");
            case GS -> System.getenv("GS_BASE_LOCATION");
            case ABFSS -> System.getenv("ABFSS_BASE_LOCATION");
        };
    }

    @Getter
    public enum LocationType {
        FILE("file://"),
        S3(System.getenv("S3_BASE_LOCATION")),
        GS(System.getenv("GS_BASE_LOCATION")),
        ABFSS(System.getenv("ABFSS_BASE_LOCATION"));

        private final String baseLocation;

        @SneakyThrows
        LocationType(String baseLocation) {
            this.baseLocation = Objects.equals(baseLocation, "file://") ? Files.createTempDirectory("uc-integration-tests").toFile().getAbsolutePath()
                    : baseLocation;
        }

        public boolean isEnabled() {
            return this.baseLocation != null;
        }
    }

    static List<LocationType> locationTypes() {
        return Arrays.stream(LocationType.values()).filter(LocationType::isEnabled).toList();
    }
}
