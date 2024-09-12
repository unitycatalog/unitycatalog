package io.unitycatalog.integrationtests;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

import java.util.Arrays;
import java.util.List;

public class BaseSparkTest {
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
                        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.connectors.spark.UCSingleCatalog")
                        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        for (String catalog : catalogs) {
            String catalogConf = "spark.sql.catalog." + catalog;
            builder =
                    builder
                            .config(catalogConf, "io.unitycatalog.connectors.spark.UCSingleCatalog")
                            .config(catalogConf + ".uri", ServerUrl)
                            .config(catalogConf + ".token", AuthToken);
        }
        if (catalogs.length > 0) {
            builder.config("spark.sql.defaultCatalog", catalogs[0]);
        }
        return builder.getOrCreate();
    }

    public enum LocationType {
        FILE,
        S3,
        GS,
    }

    static List<LocationType> locationTypes() {
        return Arrays.stream(LocationType.values()).toList();
    }
}
