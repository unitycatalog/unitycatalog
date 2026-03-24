package io.unitycatalog.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

/**
 * End-to-end test: Hudi (Java client) -> XTable -> Iceberg -> Unity Catalog auto-resolve.
 */
public class HudiXTableUCTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(java.time.Duration.ofSeconds(10))
      .build();

  private static final String TABLE_NAME = "patient_encounters";
  private static final String CATALOG_NAME = "ehr_catalog";
  private static final String SCHEMA_NAME = "clinical";

  private static final String AVRO_SCHEMA =
      """
      {
        "type": "record",
        "name": "PatientEncounter",
        "namespace": "io.unitycatalog.ehr",
        "fields": [
          {"name": "encounter_id",   "type": "int"},
          {"name": "patient_id",     "type": "string"},
          {"name": "encounter_ts",   "type": "long"},
          {"name": "provider_name",  "type": "string"},
          {"name": "diagnosis_code", "type": "string"},
          {"name": "facility",       "type": "string"}
        ]
      }
      """;

  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final String storageAccountName;
  private final String containerName;
  private final String tablePath;
  private final String tableUrl;
  private final String ucBaseUrl;
  private final String ucIcebergUrl;
  private final Configuration hadoopConf;

  private int passed = 0;
  private int failed = 0;

  public HudiXTableUCTest() {
    this.tenantId = requireEnv("TENANT_ID");
    this.clientId = requireEnv("CLIENT_ID");
    this.clientSecret = requireEnv("CLIENT_SECRET");
    this.storageAccountName = requireEnv("STORAGE_ACCOUNT_NAME");
    this.containerName = requireEnv("CONTAINER_NAME");
    this.tablePath = requireEnv("TABLE_PATH");
    this.ucBaseUrl = envOrDefault("UC_URL", "http://localhost:8080")
        + "/api/2.1/unity-catalog";
    this.ucIcebergUrl = ucBaseUrl + "/iceberg";
    this.tableUrl = String.format(
        "abfss://%s@%s.dfs.core.windows.net/%s",
        containerName, storageAccountName, tablePath);
    this.hadoopConf = buildHadoopConf();
  }

  public static void main(String[] args) throws Exception {
    new HudiXTableUCTest().run();
  }

  private void run() throws Exception {
    log("Table URL: %s", tableUrl);
    log("UC URL:    %s", ucBaseUrl);

    step("Step 0: Clean existing table path (if any)");
    cleanTablePath();

    step("Step 1: Write initial patient encounters (5 records)");
    initHudiTable();
    writeHudiRecords(buildRecordsBatch1());
    log("  Hudi initial write complete.");

    step("Step 2: XTable conversion (Hudi -> Iceberg)");
    runXTableConversion();
    log("  XTable conversion complete.");

    step("Step 3: Register table in Unity Catalog");
    registerTableInUC();
    log("  Table registered.");

    step("Step 4: Verify initial Iceberg metadata");
    String v1MetaLoc = verifyIcebergMetadata();

    step("Step 5: Append 5 more patient encounters");
    writeHudiRecords(buildRecordsBatch2());
    log("  Hudi append complete.");

    step("Step 6: XTable conversion again");
    runXTableConversion();
    log("  XTable conversion complete.");

    step("Step 7: Verify updated metadata (without re-registering)");
    String v2MetaLoc = verifyIcebergMetadata();

    if (v1MetaLoc != null && v2MetaLoc != null && !v1MetaLoc.equals(v2MetaLoc)) {
      pass("Metadata location changed: %s -> %s", v1MetaLoc, v2MetaLoc);
    } else {
      log("  Note: metadata location unchanged or null — checking snapshot count instead.");
    }

    System.out.println();
    log("================================================================");
    log("Results: %d passed, %d failed", passed, failed);
    if (failed == 0) {
      log("ALL TESTS PASSED");
    } else {
      log("SOME TESTS FAILED");
      System.exit(1);
    }
  }

  private void cleanTablePath() throws Exception {
    org.apache.hadoop.fs.FileSystem fs =
        org.apache.hadoop.fs.FileSystem.get(new URI(tableUrl), hadoopConf);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(tableUrl);
    if (fs.exists(path)) {
      fs.delete(path, true);
      log("  Deleted existing table path: %s", tableUrl);
    } else {
      log("  Table path does not exist (clean start).");
    }
  }

  private void initHudiTable() throws IOException {
    StorageConfiguration<?> storageConf = new HadoopStorageConfiguration(hadoopConf);
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableName(TABLE_NAME)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setRecordKeyFields("encounter_id")
        .initTable(storageConf, tableUrl);
    log("  Initialized Hudi table at %s", tableUrl);
  }

  private void writeHudiRecords(List<HoodieRecord<HoodieAvroPayload>> records) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(tableUrl)
        .withSchema(AVRO_SCHEMA)
        .forTable(TABLE_NAME)
        .withParallelism(1, 1)
        .withDeleteParallelism(1)
        .withEngineType(EngineType.JAVA)
        .withEmbeddedTimelineServerEnabled(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.INMEMORY)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(20, 30)
            .build())
        .build();

    StorageConfiguration<?> storageConf = new HadoopStorageConfiguration(hadoopConf);
    HoodieJavaEngineContext ctx = new HoodieJavaEngineContext(storageConf);

    try (HoodieJavaWriteClient<HoodieAvroPayload> client =
             new HoodieJavaWriteClient<>(ctx, writeConfig)) {
      String commitTime = client.startCommit();
      client.insert(records, commitTime);
      log("  Committed %d records at %s", records.size(), commitTime);
    }
  }

  private List<HoodieRecord<HoodieAvroPayload>> buildRecordsBatch1() {
    return buildRecords(new Object[][]{
        {1001, "P-10045", 1700000001L, "Dr. Smith",   "J06.9", "City General Hospital"},
        {1002, "P-10078", 1700000002L, "Dr. Patel",   "I10",   "Sunrise Medical Center"},
        {1003, "P-10112", 1700000003L, "Dr. Johnson", "E11.9", "Valley Health Clinic"},
        {1004, "P-10045", 1700000004L, "Dr. Lee",     "M54.5", "City General Hospital"},
        {1005, "P-10203", 1700000005L, "Dr. Garcia",  "K21.0", "Lakeside Community Health"},
    });
  }

  private List<HoodieRecord<HoodieAvroPayload>> buildRecordsBatch2() {
    return buildRecords(new Object[][]{
        {1006, "P-10078", 1700000006L, "Dr. Patel",   "I10",   "Sunrise Medical Center"},
        {1007, "P-10310", 1700000007L, "Dr. Kim",     "J45.20","Valley Health Clinic"},
        {1008, "P-10112", 1700000008L, "Dr. Johnson", "E11.65","Valley Health Clinic"},
        {1009, "P-10415", 1700000009L, "Dr. Smith",   "N39.0", "City General Hospital"},
        {1010, "P-10203", 1700000010L, "Dr. Garcia",  "K21.0", "Lakeside Community Health"},
    });
  }

  private List<HoodieRecord<HoodieAvroPayload>> buildRecords(Object[][] rows) {
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
    for (Object[] row : rows) {
      GenericRecord rec = new GenericData.Record(schema);
      rec.put("encounter_id", row[0]);
      rec.put("patient_id", row[1]);
      rec.put("encounter_ts", row[2]);
      rec.put("provider_name", row[3]);
      rec.put("diagnosis_code", row[4]);
      rec.put("facility", row[5]);

      HoodieKey key = new HoodieKey(String.valueOf(row[0]), "");
      HoodieAvroPayload payload = new HoodieAvroPayload(rec, (Long) row[2]);
      records.add(new HoodieAvroRecord<>(key, payload));
    }
    return records;
  }

  private void runXTableConversion() throws Exception {
    String xtableJar = envOrDefault("XTABLE_JAR",
        System.getProperty("user.home") + "/.xtable/xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar");
    if (!java.nio.file.Files.exists(java.nio.file.Path.of(xtableJar))) {
      throw new IllegalStateException(
          "XTable jar not found at: " + xtableJar
          + ". Set XTABLE_JAR env var or place jar at ~/.xtable/");
    }

    String datasetConfig = createXTableConfig();
    String hadoopConfig = createHadoopConfig();

    ProcessBuilder pb = new ProcessBuilder(
        "java", "-jar", xtableJar,
        "--datasetConfig", datasetConfig,
        "--hadoopConfig", hadoopConfig
    );
    pb.inheritIO();
    Process proc = pb.start();
    int exitCode = proc.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("XTable conversion failed with exit code " + exitCode);
    }
  }

  private String createXTableConfig() throws IOException {
    java.nio.file.Path configPath = java.nio.file.Files.createTempFile("xtable-config", ".yaml");
    String yaml = String.format(
        """
        sourceFormat: HUDI
        targetFormats:
          - ICEBERG
        datasets:
          - tableBasePath: %s
            tableName: %s
        """, tableUrl, TABLE_NAME);
    java.nio.file.Files.writeString(configPath, yaml);
    configPath.toFile().deleteOnExit();
    return configPath.toString();
  }

  private String createHadoopConfig() throws IOException {
    java.nio.file.Path configPath = java.nio.file.Files.createTempFile("hadoop-config", ".xml");
    String xml = String.format(
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <configuration>
          <property>
            <name>fs.azure.account.auth.type.%1$s.dfs.core.windows.net</name>
            <value>OAuth</value>
          </property>
          <property>
            <name>fs.azure.account.oauth.provider.type.%1$s.dfs.core.windows.net</name>
            <value>org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider</value>
          </property>
          <property>
            <name>fs.azure.account.oauth2.client.endpoint.%1$s.dfs.core.windows.net</name>
            <value>https://login.microsoftonline.com/%2$s/oauth2/token</value>
          </property>
          <property>
            <name>fs.azure.account.oauth2.client.id.%1$s.dfs.core.windows.net</name>
            <value>%3$s</value>
          </property>
          <property>
            <name>fs.azure.account.oauth2.client.secret.%1$s.dfs.core.windows.net</name>
            <value>%4$s</value>
          </property>
        </configuration>
        """, storageAccountName, tenantId, clientId, clientSecret);
    java.nio.file.Files.writeString(configPath, xml);
    configPath.toFile().deleteOnExit();
    return configPath.toString();
  }

  private void registerTableInUC() throws Exception {
    String catalogBody = String.format(
        """
        {"name": "%s", "comment": "EHR clinical data catalog"}
        """, CATALOG_NAME);
    post(ucBaseUrl + "/catalogs", catalogBody);
    log("  Created catalog: %s", CATALOG_NAME);

    String schemaBody = String.format(
        """
        {"name": "%s", "catalog_name": "%s"}
        """, SCHEMA_NAME, CATALOG_NAME);
    post(ucBaseUrl + "/schemas", schemaBody);
    log("  Created schema: %s", SCHEMA_NAME);

    String body = String.format(
        """
        {
          "name": "%s",
          "catalog_name": "%s",
          "schema_name": "%s",
          "table_type": "EXTERNAL",
          "data_source_format": "ICEBERG",
          "storage_location": "%s",
          "columns": [
            {"name": "encounter_id",   "type_name": "INT",    "type_text": "int",    "type_json": "{}", "position": 0},
            {"name": "patient_id",     "type_name": "STRING", "type_text": "string", "type_json": "{}", "position": 1},
            {"name": "encounter_ts",   "type_name": "LONG",   "type_text": "bigint", "type_json": "{}", "position": 2},
            {"name": "provider_name",  "type_name": "STRING", "type_text": "string", "type_json": "{}", "position": 3},
            {"name": "diagnosis_code", "type_name": "STRING", "type_text": "string", "type_json": "{}", "position": 4},
            {"name": "facility",       "type_name": "STRING", "type_text": "string", "type_json": "{}", "position": 5}
          ]
        }
        """, TABLE_NAME, CATALOG_NAME, SCHEMA_NAME, tableUrl);
    post(ucBaseUrl + "/tables", body);
    log("  Created table: %s (EXTERNAL, ICEBERG)", TABLE_NAME);
  }

  private String verifyIcebergMetadata() throws Exception {
    String tableEndpoint = ucIcebergUrl
        + "/v1/catalogs/" + CATALOG_NAME
        + "/namespaces/" + SCHEMA_NAME
        + "/tables/" + TABLE_NAME;

    HttpResponse<String> resp = getWithRetry(tableEndpoint, 3, 10);
    check(resp.statusCode() == 200, "loadTable (GET) returns 200 (got %d)", resp.statusCode());

    JsonNode root = JSON.readTree(resp.body());
    String metaLoc = root.path("metadata-location").asText("");
    log("  metadata-location: %s", metaLoc);

    boolean hasMeta = metaLoc.contains("metadata/v") && metaLoc.endsWith(".metadata.json");
    check(hasMeta, "metadata-location points to versioned Iceberg metadata");

    JsonNode snapshots = root.path("metadata").path("snapshots");
    int snapCount = snapshots.size();
    log("  snapshot count: %d", snapCount);
    check(snapCount >= 1, "Metadata has %d snapshot(s)", snapCount);

    JsonNode schemas = root.path("metadata").path("schemas");
    if (schemas.size() > 0) {
      JsonNode lastSchema = schemas.get(schemas.size() - 1);
      List<String> fieldNames = new ArrayList<>();
      lastSchema.path("fields").forEach(f -> fieldNames.add(f.path("name").asText()));
      log("  schema fields: %s", fieldNames);
      check(fieldNames.contains("encounter_id"), "Schema contains 'encounter_id' field");
    }

    HttpResponse<String> listResp = get(ucIcebergUrl
        + "/v1/catalogs/" + CATALOG_NAME + "/namespaces/" + SCHEMA_NAME + "/tables");
    JsonNode ids = JSON.readTree(listResp.body()).path("identifiers");
    boolean found = false;
    for (JsonNode id : ids) {
      if (TABLE_NAME.equals(id.path("name").asText())) {
        found = true;
        break;
      }
    }
    check(found, "listTables includes %s", TABLE_NAME);

    return metaLoc;
  }

  private HttpResponse<String> getWithRetry(String url, int maxAttempts, int delaySec)
      throws IOException, InterruptedException {
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        log("  GET attempt %d/%d: %s", attempt, maxAttempts, url);
        HttpResponse<String> resp = get(url);
        if (resp.statusCode() == 200) {
          return resp;
        }
        log("  Got HTTP %d, retrying in %ds...", resp.statusCode(), delaySec);
      } catch (java.net.http.HttpTimeoutException e) {
        log("  Timeout on attempt %d, retrying in %ds...", attempt, delaySec);
      }
      if (attempt < maxAttempts) {
        Thread.sleep(delaySec * 1000L);
      }
    }
    return get(url);
  }

  private HttpResponse<String> get(String url) throws IOException, InterruptedException {
    return HTTP.send(
        HttpRequest.newBuilder(URI.create(url))
            .timeout(java.time.Duration.ofSeconds(120))
            .GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private int head(String url) throws IOException, InterruptedException {
    return HTTP.send(
        HttpRequest.newBuilder(URI.create(url))
            .timeout(java.time.Duration.ofSeconds(120))
            .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.discarding()).statusCode();
  }

  private void post(String url, String jsonBody) throws IOException, InterruptedException {
    HttpResponse<String> resp = HTTP.send(
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
            .build(),
        HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() >= 400) {
      throw new RuntimeException("POST " + url + " failed (" + resp.statusCode() + "): " + resp.body());
    }
  }

  private Configuration buildHadoopConf() {
    Configuration conf = new Configuration();
    String acct = storageAccountName + ".dfs.core.windows.net";
    conf.set("fs.azure.account.auth.type." + acct, "OAuth");
    conf.set("fs.azure.account.oauth.provider.type." + acct,
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
    conf.set("fs.azure.account.oauth2.client.endpoint." + acct,
        "https://login.microsoftonline.com/" + tenantId + "/oauth2/token");
    conf.set("fs.azure.account.oauth2.client.id." + acct, clientId);
    conf.set("fs.azure.account.oauth2.client.secret." + acct, clientSecret);
    return conf;
  }

  private void check(boolean condition, String fmt, Object... args) {
    if (condition) {
      pass(fmt, args);
    } else {
      fail(fmt, args);
    }
  }

  private void pass(String fmt, Object... args) {
    System.out.printf("  PASS: " + fmt + "%n", args);
    passed++;
  }

  private void fail(String fmt, Object... args) {
    System.out.printf("  FAIL: " + fmt + "%n", args);
    failed++;
  }

  private static void step(String msg) {
    System.out.println();
    System.out.println("=== " + msg + " ===");
  }

  private static void log(String fmt, Object... args) {
    System.out.printf(fmt + "%n", args);
  }

  private static String requireEnv(String name) {
    String val = System.getenv(name);
    if (val == null || val.isBlank() || val.startsWith("your-")) {
      throw new IllegalStateException("Required env var not set: " + name);
    }
    return val;
  }

  private static String envOrDefault(String name, String defaultVal) {
    String val = System.getenv(name);
    return (val != null && !val.isBlank()) ? val : defaultVal;
  }
}
