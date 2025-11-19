package io.unitycatalog.spark.oauth;

import io.unitycatalog.spark.UCSingleCatalog;
import java.util.List;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestOAuth {

  @Test
  public void testOAuth() {
    String serverUrl = System.getenv("UC_URI");
    String oauthUri = System.getenv("OAUTH_URI");
    String oauthClientId = System.getenv("OAUTH_CLIENT_ID");
    String oauthClientSecret = System.getenv("OAUTH_CLIENT_SECRET");

    SparkSession spark = SparkSession.builder()
        .appName("test-credential-renewal")
        .master("local[1]") // Make it single-threaded explicitly.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.catalog.main", UCSingleCatalog.class.getName())
        .config("spark.sql.catalog.main.uri", serverUrl)
        .config("spark.sql.catalog.main.warehouse", "main")
        .config("spark.sql.catalog.main.renewCredential.enabled", "true")
        .config("spark.sql.catalog.main.oauthUri", oauthUri)
        .config("spark.sql.catalog.main.oauthClientId", oauthClientId)
        .config("spark.sql.catalog.main.oauthClientSecret", oauthClientSecret)
        .config("fs.s3.impl", S3AFileSystem.class.getName())
        .getOrCreate();

    List<Row> rows = spark.sql("SELECT * FROM main.demo_zh.managedcctable").collectAsList();
    for (Row row : rows) {
      System.out.println(row);
    }
  }
}
