package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.TemporaryCredentials;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for credential fallback behavior in UCSingleCatalog.
 *
 * <h2>Test Coverage</h2>
 *
 * This test suite verifies the credential fallback chain when loading Unity Catalog tables:
 *
 * <ol>
 *   <li>First attempt: READ_WRITE credentials
 *   <li>Fallback: READ credentials
 *   <li>Final fallback: No credentials (behavior depends on SSP config)
 * </ol>
 *
 * <h2>Test Matrix</h2>
 *
 * Tests cover all combinations of:
 *
 * <ul>
 *   <li><b>Credential Scenarios:</b>
 *       <ul>
 *         <li>READ_WRITE succeeds (1 API call)
 *         <li>READ_WRITE fails, READ succeeds (2 API calls)
 *         <li>Both fail (2 API calls)
 *       </ul>
 *   <li><b>SSP Modes:</b>
 *       <ul>
 *         <li>Disabled (default): Fail-fast when credentials unavailable
 *         <li>Enabled: Allow empty credentials for FGAC scenarios
 *       </ul>
 *   <li><b>Table Types:</b>
 *       <ul>
 *         <li>MANAGED tables
 *         <li>EXTERNAL tables
 *       </ul>
 * </ul>
 *
 * <h2>Security Guarantees</h2>
 *
 * <ul>
 *   <li>Default behavior (SSP disabled) is secure: fails fast without credentials
 *   <li>Empty credentials only allowed when SSP explicitly enabled
 *   <li>Clear error messages include table identifiers for debugging
 * </ul>
 *
 * <h2>Test Architecture</h2>
 *
 * <ul>
 *   <li>Uses mocking to simulate API failures without live server
 *   <li>Separate SparkSession per SSP mode for proper isolation
 *   <li>Parameterized tests to ensure complete matrix coverage
 *   <li>Reflection-based mock injection for internal API testing
 * </ul>
 *
 * @see UCSingleCatalog
 * @see io.unitycatalog.spark.auth.CredPropsUtil
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class UCSingleCatalogCredentialFallbackTest {

  private SparkSession createSparkSessionForMode(SspMode mode) {
    return SparkSession.builder()
        .master("local[*]")
        .appName("test-ssp-" + mode.name().toLowerCase() + "-" + System.currentTimeMillis())
        .config(
            "spark.databricks.delta.catalog.enableServerSidePlanning",
            String.valueOf(mode.configValue))
        .getOrCreate();
  }

  /**
   * Parameterized test covering the full credential fallback matrix: - 3 credential scenarios (RW
   * succeeds, RW fails/R succeeds, both fail) - 2 SSP modes (enabled, disabled) = 6 test
   * combinations
   */
  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("credentialFallbackScenarios")
  @DisplayName("Credential fallback behavior")
  void testCredentialFallback(SspMode sspMode, CredentialScenario scenario) throws Exception {

    // Create the correct SparkSession for this SSP mode
    SparkSession session = createSparkSessionForMode(sspMode);

    try {
      // Create fresh catalog fixture
      CatalogTestFixture fixture = CatalogTestFixture.create();

      // Setup test data
      TableInfo testTable =
          TestData.createTable(
              TestData.DEFAULT_TABLE_NAME,
              TestData.DEFAULT_SCHEMA_NAME,
              TestData.DEFAULT_STORAGE_LOCATION);
      TemporaryCredentials rwCreds = TestData.createCredentials(TestData.RW_TOKEN);
      TemporaryCredentials readCreds = TestData.createCredentials(TestData.READ_TOKEN);

      // Configure mocks based on scenario
      when(fixture.mockTablesApi.getTable(any(), any(), any())).thenReturn(testTable);
      scenario.configureMocks(fixture.mockCredentialsApi, rwCreds, readCreds);

      // Execute and assert based on expected behavior
      Identifier tableId =
          TestData.tableIdentifier(TestData.DEFAULT_SCHEMA_NAME, TestData.DEFAULT_TABLE_NAME);

      if (shouldSucceed(scenario, sspMode)) {
        // Table load should succeed
        Table result = fixture.catalog.loadTable(tableId);
        assertThat(result).isNotNull();
      } else {
        // Table load should throw exception (SSP disabled + both creds fail)
        assertThatThrownBy(() -> fixture.catalog.loadTable(tableId))
            .isInstanceOf(ApiException.class)
            .hasMessageContaining("generateTemporaryTableCredentials failed");
      }

      // Verify expected number of API calls
      verify(fixture.mockCredentialsApi, times(scenario.expectedApiCalls))
          .generateTemporaryTableCredentials(any());
    } finally {
      session.stop();
    }
  }

  /** Determines if table load should succeed given scenario and SSP mode. */
  private boolean shouldSucceed(CredentialScenario scenario, SspMode sspMode) {
    // Always succeeds if credentials available
    if (scenario.succeeds) {
      return true;
    }
    // When both creds fail: succeeds only if SSP enabled
    return sspMode == SspMode.ENABLED;
  }

  /** Provides test parameters: all combinations of SSP mode and credential scenario. */
  static Stream<Arguments> credentialFallbackScenarios() {
    return Stream.of(SspMode.values())
        .flatMap(
            sspMode ->
                Stream.of(CredentialScenario.values())
                    .map(scenario -> Arguments.of(sspMode, scenario)));
  }

  @ParameterizedTest
  @EnumSource(SspMode.class)
  @DisplayName("Exception message contains table identifier when credentials fail")
  void testExceptionMessageContainsTableInfo(SspMode sspMode) throws Exception {
    SparkSession session = createSparkSessionForMode(sspMode);

    try {
      CatalogTestFixture fixture = CatalogTestFixture.create();

      // Use distinctive table name to verify it appears in error
      String tableName = "important_table";
      String schemaName = "sensitive_schema";
      TableInfo testTable =
          TestData.createTable(tableName, schemaName, TestData.DEFAULT_STORAGE_LOCATION);

      when(fixture.mockTablesApi.getTable(any(), any(), any())).thenReturn(testTable);
      when(fixture.mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied"))
          .thenThrow(new ApiException("Permission denied"));

      Identifier tableId = TestData.tableIdentifier(schemaName, tableName);

      if (sspMode == SspMode.DISABLED) {
        // Should throw with table name in message
        assertThatThrownBy(() -> fixture.catalog.loadTable(tableId))
            .isInstanceOf(ApiException.class)
            .hasMessageContaining(tableName);
      } else {
        // SSP enabled - succeeds with empty credentials, no exception
        Table result = fixture.catalog.loadTable(tableId);
        assertThat(result).isNotNull();
      }
    } finally {
      session.stop();
    }
  }

  /**
   * Parameterized test covering credential fallback for all table types: - 2 table types (MANAGED,
   * EXTERNAL) - 3 credential scenarios (RW succeeds, RW fails/R succeeds, both fail) - 2 SSP modes
   * (enabled, disabled) = 12 test combinations
   */
  @ParameterizedTest(name = "{0} table, {1}, {2}")
  @MethodSource("tableTypeScenarios")
  @DisplayName("Credential fallback works for all table types")
  void testCredentialFallbackByTableType(
      TableTypeScenario tableType, SspMode sspMode, CredentialScenario scenario) throws Exception {

    SparkSession session = createSparkSessionForMode(sspMode);

    try {
      CatalogTestFixture fixture = CatalogTestFixture.create();

      // Create table with specific type
      TableInfo testTable =
          TestData.createTable(
              TestData.DEFAULT_TABLE_NAME,
              TestData.DEFAULT_SCHEMA_NAME,
              TestData.DEFAULT_STORAGE_LOCATION);
      testTable.setTableType(tableType.tableType);

      TemporaryCredentials rwCreds = TestData.createCredentials(TestData.RW_TOKEN);
      TemporaryCredentials readCreds = TestData.createCredentials(TestData.READ_TOKEN);

      // Configure mocks based on scenario
      when(fixture.mockTablesApi.getTable(any(), any(), any())).thenReturn(testTable);
      scenario.configureMocks(fixture.mockCredentialsApi, rwCreds, readCreds);

      // Execute and assert based on expected behavior
      Identifier tableId =
          TestData.tableIdentifier(TestData.DEFAULT_SCHEMA_NAME, TestData.DEFAULT_TABLE_NAME);

      if (shouldSucceed(scenario, sspMode)) {
        // Table load should succeed
        Table result = fixture.catalog.loadTable(tableId);
        assertThat(result).isNotNull();
      } else {
        // Table load should throw exception (SSP disabled + both creds fail)
        assertThatThrownBy(() -> fixture.catalog.loadTable(tableId))
            .isInstanceOf(ApiException.class)
            .hasMessageContaining("generateTemporaryTableCredentials failed");
      }

      // Verify expected number of API calls
      verify(fixture.mockCredentialsApi, times(scenario.expectedApiCalls))
          .generateTemporaryTableCredentials(any());
    } finally {
      session.stop();
    }
  }

  static Stream<Arguments> tableTypeScenarios() {
    return Stream.of(TableTypeScenario.values())
        .flatMap(
            tableType ->
                Stream.of(SspMode.values())
                    .flatMap(
                        sspMode ->
                            Stream.of(CredentialScenario.values())
                                .map(scenario -> Arguments.of(tableType, sspMode, scenario))));
  }

  @Test
  @DisplayName("Gracefully handle missing SparkSession")
  void testMissingSparkSession() throws Exception {
    // Clear active session to simulate edge case
    SparkSession.clearActiveSession();

    CatalogTestFixture fixture = CatalogTestFixture.create();

    TableInfo testTable =
        TestData.createTable(
            TestData.DEFAULT_TABLE_NAME,
            TestData.DEFAULT_SCHEMA_NAME,
            TestData.DEFAULT_STORAGE_LOCATION);

    when(fixture.mockTablesApi.getTable(any(), any(), any())).thenReturn(testTable);
    when(fixture.mockCredentialsApi.generateTemporaryTableCredentials(any()))
        .thenThrow(new ApiException("Permission denied"))
        .thenThrow(new ApiException("Permission denied"));

    Identifier tableId =
        TestData.tableIdentifier(TestData.DEFAULT_SCHEMA_NAME, TestData.DEFAULT_TABLE_NAME);

    // Should fail-fast when no SparkSession (defaults to SSP disabled)
    assertThatThrownBy(() -> fixture.catalog.loadTable(tableId))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("generateTemporaryTableCredentials failed");
  }

  @Test
  @DisplayName("Handle invalid SSP config value gracefully")
  void testInvalidSspConfigValue() throws Exception {
    // Create session with invalid config value
    SparkSession invalidSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("test-invalid-config")
            .config("spark.databricks.delta.catalog.enableServerSidePlanning", "invalid-value")
            .getOrCreate();

    try {
      SparkSession.setActiveSession(invalidSession);

      CatalogTestFixture fixture = CatalogTestFixture.create();

      TableInfo testTable =
          TestData.createTable(
              TestData.DEFAULT_TABLE_NAME,
              TestData.DEFAULT_SCHEMA_NAME,
              TestData.DEFAULT_STORAGE_LOCATION);

      when(fixture.mockTablesApi.getTable(any(), any(), any())).thenReturn(testTable);
      when(fixture.mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied"))
          .thenThrow(new ApiException("Permission denied"));

      Identifier tableId =
          TestData.tableIdentifier(TestData.DEFAULT_SCHEMA_NAME, TestData.DEFAULT_TABLE_NAME);

      // Should fail-fast when invalid config (defaults to SSP disabled)
      assertThatThrownBy(() -> fixture.catalog.loadTable(tableId))
          .isInstanceOf(ApiException.class)
          .hasMessageContaining("generateTemporaryTableCredentials failed");
    } finally {
      invalidSession.stop();
    }
  }

  /** Server-Side Planning configuration modes. */
  enum SspMode {
    ENABLED(true, "SSP enabled"),
    DISABLED(false, "SSP disabled");

    final boolean configValue;
    final String displayName;

    SspMode(boolean configValue, String displayName) {
      this.configValue = configValue;
      this.displayName = displayName;
    }

    @Override
    public String toString() {
      return displayName;
    }
  }

  /** Credential generation scenarios for testing the fallback chain. */
  enum CredentialScenario {
    /** READ_WRITE credentials succeed on first attempt */
    READ_WRITE_SUCCEEDS(
        "RW succeeds",
        1, // expected API calls
        true, // succeeds
        false // not read-only
        ),

    /** READ_WRITE fails, READ succeeds on second attempt */
    READ_WRITE_FAILS_READ_SUCCEEDS(
        "RW fails, READ succeeds", 2, true, true // read-only
        ),

    /** Both READ_WRITE and READ fail */
    BOTH_FAIL("Both fail", 2, false, false);

    final String displayName;
    final int expectedApiCalls;
    final boolean succeeds;
    final boolean readOnly;

    CredentialScenario(
        String displayName, int expectedApiCalls, boolean succeeds, boolean readOnly) {
      this.displayName = displayName;
      this.expectedApiCalls = expectedApiCalls;
      this.succeeds = succeeds;
      this.readOnly = readOnly;
    }

    /** Configure mocks for this scenario */
    void configureMocks(
        TemporaryCredentialsApi mockApi,
        TemporaryCredentials rwCreds,
        TemporaryCredentials readCreds)
        throws ApiException {
      switch (this) {
        case READ_WRITE_SUCCEEDS:
          when(mockApi.generateTemporaryTableCredentials(any())).thenReturn(rwCreds);
          break;
        case READ_WRITE_FAILS_READ_SUCCEEDS:
          when(mockApi.generateTemporaryTableCredentials(any()))
              .thenThrow(new ApiException("Permission denied for READ_WRITE"))
              .thenReturn(readCreds);
          break;
        case BOTH_FAIL:
          when(mockApi.generateTemporaryTableCredentials(any()))
              .thenThrow(new ApiException("Permission denied for READ_WRITE"))
              .thenThrow(new ApiException("Permission denied for READ"));
          break;
      }
    }

    @Override
    public String toString() {
      return displayName;
    }
  }

  /** Table type scenarios for testing. */
  enum TableTypeScenario {
    MANAGED(TableType.MANAGED),
    EXTERNAL(TableType.EXTERNAL);

    final TableType tableType;

    TableTypeScenario(TableType tableType) {
      this.tableType = tableType;
    }

    @Override
    public String toString() {
      return tableType.getValue();
    }
  }

  /**
   * Test fixture that encapsulates catalog setup and mock configuration. Handles the boilerplate of
   * creating catalogs, injecting mocks, and managing lifecycle.
   */
  static class CatalogTestFixture {
    final UCSingleCatalog catalog;
    final TemporaryCredentialsApi mockCredentialsApi;
    final TablesApi mockTablesApi;

    private CatalogTestFixture() throws Exception {
      // Disable DeltaCatalog loading to avoid unwanted dependencies
      UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);

      this.catalog = new UCSingleCatalog();
      this.mockCredentialsApi = mock(TemporaryCredentialsApi.class);
      this.mockTablesApi = mock(TablesApi.class);

      // Initialize catalog
      Map<String, String> options = new HashMap<>();
      options.put("uri", "http://localhost:8080");
      options.put("token", "test-token");
      catalog.initialize("unity", new CaseInsensitiveStringMap(options));

      // Inject mocked APIs using reflection
      injectMockedApis(catalog, mockCredentialsApi, mockTablesApi);
    }

    static CatalogTestFixture create() throws Exception {
      return new CatalogTestFixture();
    }

    private static void injectMockedApis(
        UCSingleCatalog catalog, TemporaryCredentialsApi credApi, TablesApi tablesApi)
        throws Exception {
      Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      Object delegate = delegateField.get(catalog);
      injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", credApi);
      injectMockedApiIntoObject(delegate, "tablesApi", tablesApi);
    }

    private static void injectMockedApiIntoObject(Object target, String fieldName, Object mockApi)
        throws Exception {
      Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, mockApi);
    }
  }

  /** Test data constants and factory methods. */
  interface TestData {
    String DEFAULT_TABLE_NAME = "test_table";
    String DEFAULT_SCHEMA_NAME = "test_schema";
    String DEFAULT_CATALOG_NAME = "unity";
    String DEFAULT_STORAGE_LOCATION = "s3://bucket/path";

    String RW_TOKEN = "rw-token";
    String READ_TOKEN = "read-token";

    static Identifier tableIdentifier(String schema, String table) {
      return Identifier.of(new String[] {schema}, table);
    }

    static TableInfo createTable(String tableName, String schemaName, String location) {
      TableInfo table = new TableInfo();
      table.setName(tableName);
      table.setSchemaName(schemaName);
      table.setCatalogName(DEFAULT_CATALOG_NAME);
      table.setTableType(TableType.MANAGED);
      table.setDataSourceFormat(DataSourceFormat.DELTA);
      table.setStorageLocation(location);
      table.setTableId(UUID.randomUUID().toString());
      table.setColumns(Collections.emptyList());
      table.setProperties(Collections.emptyMap());
      table.setCreatedAt(System.currentTimeMillis());
      return table;
    }

    static TemporaryCredentials createCredentials(String token) {
      TemporaryCredentials creds = new TemporaryCredentials();
      AwsCredentials awsCreds = new AwsCredentials();
      awsCreds.setAccessKeyId("test-access-key");
      awsCreds.setSecretAccessKey("test-secret-key");
      awsCreds.setSessionToken(token);
      creds.setAwsTempCredentials(awsCreds);
      creds.setExpirationTime(System.currentTimeMillis() + 3600000);
      return creds;
    }
  }
}
