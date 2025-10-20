package io.unitycatalog.integrationtests;

import static java.lang.String.format;


import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SparkCredentialRenewalTest extends BaseSparkTest {
  private static final String BASE_TABLE_NAME = "numbers";
  private final String RUN_ID = UUID.randomUUID().toString();
  private static final String SCHEMA = RandomStringUtils.randomAlphabetic(8);

  static String getTableName(LocationType locationType) {
    return format("%s_%s", BASE_TABLE_NAME, locationType.name());
  }

  @BeforeAll
  public static void setupSchema() {
    spark.sql(format("CREATE SCHEMA %s", SCHEMA));
    spark.sql(format("USE %s", SCHEMA));
  }

  @AfterAll
  public static void teardownSchema() {
    locationTypes().forEach(locationType ->
        spark.sql(format("DROP TABLE IF EXISTS %s", getTableName(locationType))));
    spark.sql(format("DROP SCHEMA IF EXISTS %s", SCHEMA));
  }

  @ParameterizedTest
  @MethodSource("locationTypes")
  @Order(1)
  public void insert(LocationType locationType) throws Exception {
    String baseLocation = getBaseLocation(locationType);
    String location = baseLocation + "/integration/" + RUN_ID + "/numbers";
    String table = getTableName(locationType);

    spark.sql(format("CREATE TABLE %s(as_int INT, as_double DOUBLE) USING DELTA LOCATION '%s'",
        table, location));


    // We'll use a rate source that produces rows with timestamp and value columns.
    Dataset<Row> rateStream = spark.readStream()
        .format("rate")
        .option("rowsPerSecond", 10) // adjust ingestion rate
        .load();

    // Map each row to random data
    Dataset<Row> randomData = rateStream.map(
        (MapFunction<Row, Row>) row -> {
          int asInt = ThreadLocalRandom.current().nextInt(0, 1000);
          double asDouble = ThreadLocalRandom.current().nextDouble();
          return RowFactory.create(asInt, asDouble);
        },
        RowEncoder.encoderFor(
            new StructType()
                .add("as_int", DataTypes.IntegerType)
                .add("as_double", DataTypes.DoubleType)
        )
    );

    // Write Stream to Delta Table
    StreamingQuery query = randomData.writeStream()
        .format("delta")
        .option("checkpointLocation", location + "_checkpoint")
        .outputMode("append")
        .start(location); // or use tableName with .toTable(tableName)

    // Let It Run for 40 Minutes
    // 40 min = 40 * 60 * 1000 ms = 2,400,000 ms
    query.awaitTermination(40 * 60 * 1000);

    // Optionally stop gracefully
    query.stop();
    spark.stop();
  }
}
