package io.unitycatalog.integrationtests;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public class BaseSparkTest {
    private static final String ServerUrl = "http://localhost:8080";
    private static final String AuthToken = "";
    private static final ObjectMapper mapper = new ObjectMapper();
    protected static SparkSession spark;

    @BeforeAll
    public static void setup() {
        // FIXME: catalog must be named `spark_catalog` for now, upstream issue
        spark = createSparkSessionWithCatalogs("spark_catalog");
    }

    @SneakyThrows
    protected List<String> getExpectData(String resourceFileName) {
        return mapper.readValue(
                getClass().getClassLoader().getResourceAsStream(resourceFileName),
                new TypeReference<>() {
                }
        );
    }

    protected static SparkSession createSparkSessionWithCatalogs(String... catalogs) {
        SparkSession.Builder builder =
                SparkSession.builder()
                        .appName("test")
                        .master("local[*]")
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
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
}
