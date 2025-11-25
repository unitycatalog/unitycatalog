package io.unitycatalog.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class TestTableHyphens {

  @Test
  public void testTableHyphens() {
    String uri = System.getenv().getOrDefault("UC_URI", null);
    String token = System.getenv().getOrDefault("UC_TOKEN", null);

    SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("test")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.main", UCSingleCatalog.class.getName())
        .config("spark.sql.catalog.main.uri", uri)
        .config("spark.sql.catalog.main.token", token)
        .config("spark.sql.catalog.main.warehouse", "main")
        .getOrCreate();

    long num = spark.sql("SELECT * FROM `main`.`demo_zh`.`table-with-hyphens`").count();
    System.out.println(num);

    spark.stop();
  }
}
