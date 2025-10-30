package io.unitycatalog.integrationtests;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkCredentialRenewalTest {
  private static final String ServerUri = System.getenv().getOrDefault("CATALOG_URI", "http://localhost:8080");
  private static final String AuthToken = System.getenv().getOrDefault("CATALOG_AUTH_TOKEN", "");
  private static final String CatalogName = System.getenv().getOrDefault("CATALOG_NAME", "unity");
  private static final String SchemaName = System.getenv().getOrDefault("SCHEMA_NAME", "unity");
  private static final String TableName = System.getenv().getOrDefault("TABLE_NAME", "unity");
  private static final String S3BaseLocation = System.getenv().getOrDefault("S3_BASE_LOCATION",
      "s3://bucket/key");
  private static final String RowCount = System.getenv().getOrDefault("ROW_COUNT", "10000000");

  public static SparkSession spark;
  public static String fullTable;

  @BeforeAll
  public static void beforeAll() {
    String testCatalogKey = String.format("spark.sql.catalog.%s", CatalogName);
    spark = SparkSession.builder()
        .appName("test-credential-renewal")
        .master("local[1]") // Make it single-threaded explicitly.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config(testCatalogKey, UCSingleCatalog.class.getName())
        .config(testCatalogKey + ".uri", ServerUri)
        .config(testCatalogKey + ".token", AuthToken)
        .config(testCatalogKey + ".warehouse", CatalogName)
        .config(testCatalogKey + ".renewCredential.enabled", "true")
        .getOrCreate();

    fullTable = String.format("%s.%s.%s", CatalogName, SchemaName, TableName);
    sql("CREATE TABLE %s (id INT, val STRING) USING delta LOCATION '%s/%s'",
        fullTable, S3BaseLocation, UUID.randomUUID());
  }

  @AfterAll
  public static void afterAll() {
    sql("DROP TABLE IF EXISTS %s", fullTable);
    spark.stop();
  }

  @Test
  public void test() {
    sql("INSERT INTO %s SELECT id, CONCAT('val_', id) AS val FROM range(0, %s)",
        fullTable, RowCount);

    List<Row> results = sql("SELECT COUNT(*) FROM %s", fullTable);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0).getLong(0)).isEqualTo(Long.parseLong(RowCount));
  }

  private static List<Row> sql(String statement, Object... args) {
    return spark.sql(String.format(statement, args)).collectAsList();
  }
}
