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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Unit tests for credential fallback behavior in UCSingleCatalog.
 *
 * <p>These tests verify:
 * - Credential fallback chain: READ_WRITE → READ → None
 * - Server-side planning (SSP) configuration behavior
 * - SSP disabled (default): Fail-fast with clear exception when credentials fail
 * - SSP enabled: Allow empty credentials with info logs when credentials fail
 * - Security: Ensure credentials are never bypassed unintentionally
 *
 * <p>Tests are organized into nested classes by SSP configuration:
 * - SSPDisabledTests: Tests with spark.databricks.delta.catalog.enableServerSidePlanning=false
 * - SSPEnabledTests: Tests with spark.databricks.delta.catalog.enableServerSidePlanning=true
 *
 * <p>Each nested class manages its own SparkSession lifecycle to ensure proper config isolation.
 */
public class UCSingleCatalogCredentialFallbackTest {

  /**
   * Helper method to inject mocked APIs into an object using reflection.
   */
  private static void injectMockedApiIntoObject(Object target, String fieldName, Object mockApi)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, mockApi);
  }

  /**
   * Helper method to create a minimal TableInfo for testing.
   */
  private static TableInfo createTestTable(
      String tableName, String schemaName, String storageLocation) {
    TableInfo table = new TableInfo();
    table.setName(tableName);
    table.setSchemaName(schemaName);
    table.setCatalogName("unity");
    table.setTableType(TableType.MANAGED);
    table.setDataSourceFormat(DataSourceFormat.DELTA);
    table.setStorageLocation(storageLocation);
    table.setTableId(UUID.randomUUID().toString());
    table.setColumns(Collections.emptyList());
    table.setProperties(Collections.emptyMap());
    table.setCreatedAt(System.currentTimeMillis());
    return table;
  }

  /**
   * Helper method to create test temporary credentials.
   */
  private static TemporaryCredentials createTestCredentials(String token) {
    TemporaryCredentials creds = new TemporaryCredentials();

    // Create AWS credentials
    AwsCredentials awsCreds = new AwsCredentials();
    awsCreds.setAccessKeyId("test-access-key");
    awsCreds.setSecretAccessKey("test-secret-key");
    awsCreds.setSessionToken(token);
    creds.setAwsTempCredentials(awsCreds);

    creds.setExpirationTime(System.currentTimeMillis() + 3600000);
    return creds;
  }

  /**
   * Tests with server-side planning disabled (default behavior).
   * When credentials fail, the catalog should throw an ApiException.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class SSPDisabledTests {
    private SparkSession spark;
    private UCSingleCatalog catalog;
    private TemporaryCredentialsApi mockCredentialsApi;
    private TablesApi mockTablesApi;

    @BeforeAll
    void setupSession() {
      spark = SparkSession.builder()
          .master("local[*]")
          .appName("test-ssp-disabled")
          .config("spark.databricks.delta.catalog.enableServerSidePlanning", "false")
          .getOrCreate();
    }

    @BeforeEach
    void setupCatalog() throws Exception {
      // Disable DeltaCatalog loading to avoid unwanted dependencies
      UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);

      catalog = new UCSingleCatalog();
      mockCredentialsApi = mock(TemporaryCredentialsApi.class);
      mockTablesApi = mock(TablesApi.class);

      Map<String, String> options = new HashMap<>();
      options.put("uri", "http://localhost:8080");
      options.put("token", "test-token");
      catalog.initialize("unity", new CaseInsensitiveStringMap(options));

      // Inject mocked APIs using reflection
      Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      Object delegate = delegateField.get(catalog);
      injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi);
      injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);
    }

    @AfterAll
    void teardownSession() {
      if (spark != null) {
        spark.stop();
      }
    }

    @Test
    public void testCredentialFallback_ReadWriteSucceeds() throws Exception {
      // Setup: Mock API to return valid READ_WRITE credentials
      TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials testCreds = createTestCredentials("rw-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any())).thenReturn(testCreds);

      // Execute: Load table
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully
      assertThat(result).isNotNull();

      // Verify: Only one API call was made (READ_WRITE succeeded)
      verify(mockCredentialsApi, times(1)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_ReadWriteFailsReadSucceeds() throws Exception {
      // Setup: Mock API to fail for READ_WRITE but succeed for READ
      TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials readCreds = createTestCredentials("read-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenReturn(readCreds);

      // Execute: Load table
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully with READ credentials
      assertThat(result).isNotNull();

      // Verify: Two API calls were made (READ_WRITE failed, READ succeeded)
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_BothFail_ThrowsException() throws Exception {
      // Setup: Mock API to fail both credential attempts
      TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenThrow(new ApiException("Permission denied for READ"));

      // Execute & Verify: Loading table throws exception (SSP disabled = fail-fast)
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
      assertThatThrownBy(() -> catalog.loadTable(tableId))
          .isInstanceOf(ApiException.class)
          .hasMessageContaining("generateTemporaryTableCredentials failed");

      // Verify: Two API calls were made (both READ_WRITE and READ attempted)
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_ExceptionMessageContainsTableInfo() throws Exception {
      // Setup: Mock API to fail both credential attempts
      TableInfo testTable =
          createTestTable("important_table", "sensitive_schema", "s3://bucket/path");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied"))
          .thenThrow(new ApiException("Permission denied"));

      // Execute & Verify: Exception contains table identifier
      Identifier tableId = Identifier.of(new String[] {"sensitive_schema"}, "important_table");
      assertThatThrownBy(() -> catalog.loadTable(tableId))
          .isInstanceOf(ApiException.class)
          .hasMessageContaining("important_table");
    }

    @Test
    public void testCredentialFallback_ReadOnlyCredentialsSucceed() throws Exception {
      // Setup: Mock API to fail READ_WRITE but succeed for READ
      TableInfo testTable = createTestTable("readonly_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials readCreds = createTestCredentials("read-only-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenReturn(readCreds);

      // Execute: Load table (will get READ-only credentials)
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "readonly_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully
      assertThat(result).isNotNull();

      // Verify: Fallback to READ credentials worked
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }
  }

  /**
   * Tests with server-side planning enabled.
   * When credentials fail, the catalog should proceed with empty credentials.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class SSPEnabledTests {
    private SparkSession spark;
    private UCSingleCatalog catalog;
    private TemporaryCredentialsApi mockCredentialsApi;
    private TablesApi mockTablesApi;

    @BeforeAll
    void setupSession() {
      spark = SparkSession.builder()
          .master("local[*]")
          .appName("test-ssp-enabled")
          .config("spark.databricks.delta.catalog.enableServerSidePlanning", "true")
          .getOrCreate();
    }

    @BeforeEach
    void setupCatalog() throws Exception {
      // Disable DeltaCatalog loading to avoid unwanted dependencies
      UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);

      catalog = new UCSingleCatalog();
      mockCredentialsApi = mock(TemporaryCredentialsApi.class);
      mockTablesApi = mock(TablesApi.class);

      Map<String, String> options = new HashMap<>();
      options.put("uri", "http://localhost:8080");
      options.put("token", "test-token");
      catalog.initialize("unity", new CaseInsensitiveStringMap(options));

      // Inject mocked APIs using reflection
      Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
      delegateField.setAccessible(true);
      Object delegate = delegateField.get(catalog);
      injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi);
      injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);
    }

    @AfterAll
    void teardownSession() {
      if (spark != null) {
        spark.stop();
      }
    }

    @Test
    public void testCredentialFallback_ReadWriteSucceeds() throws Exception {
      // Setup: Mock API to return valid READ_WRITE credentials
      TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials testCreds = createTestCredentials("rw-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any())).thenReturn(testCreds);

      // Execute: Load table
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully
      assertThat(result).isNotNull();

      // Verify: Only one API call was made (READ_WRITE succeeded)
      verify(mockCredentialsApi, times(1)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_ReadWriteFailsReadSucceeds() throws Exception {
      // Setup: Mock API to fail for READ_WRITE but succeed for READ
      TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials readCreds = createTestCredentials("read-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenReturn(readCreds);

      // Execute: Load table
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully with READ credentials
      assertThat(result).isNotNull();

      // Verify: Two API calls were made (READ_WRITE failed, READ succeeded)
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_BothFail_ReturnsEmptyMap() throws Exception {
      // Setup: Mock API to fail both credential attempts
      TableInfo testTable = createTestTable("fgac_table", "test_schema", "s3://bucket/path");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenThrow(new ApiException("Permission denied for READ"));

      // Execute: Load table (should succeed with empty credentials when SSP enabled)
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "fgac_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully even without credentials
      assertThat(result).isNotNull();

      // Verify: Two API calls were made (both READ_WRITE and READ attempted before fallback)
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());

      // Note: When SSP is enabled, the catalog proceeds with empty credentials
      // and logs an INFO message. Delta will handle file discovery and credential
      // vending via server-side planning.
    }

    @Test
    public void testCredentialFallback_ExceptionMessageWhenBothFail() throws Exception {
      // Setup: Mock API to fail both credential attempts
      TableInfo testTable =
          createTestTable("important_table", "sensitive_schema", "s3://bucket/path");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied"))
          .thenThrow(new ApiException("Permission denied"));

      // Execute: Load table (should succeed with empty credentials when SSP enabled)
      Identifier tableId = Identifier.of(new String[] {"sensitive_schema"}, "important_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully
      assertThat(result).isNotNull();

      // With SSP enabled, no exception is thrown - empty credentials are used instead
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }

    @Test
    public void testCredentialFallback_ReadOnlyCredentialsSucceed() throws Exception {
      // Setup: Mock API to fail READ_WRITE but succeed for READ
      TableInfo testTable = createTestTable("readonly_table", "test_schema", "s3://bucket/path");
      TemporaryCredentials readCreds = createTestCredentials("read-only-token");

      when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
          .thenReturn(testTable);
      when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
          .thenThrow(new ApiException("Permission denied for READ_WRITE"))
          .thenReturn(readCreds);

      // Execute: Load table (will get READ-only credentials)
      Identifier tableId = Identifier.of(new String[] {"test_schema"}, "readonly_table");
      Table result = catalog.loadTable(tableId);

      // Verify: Table loads successfully
      assertThat(result).isNotNull();

      // Verify: Fallback to READ credentials worked
      verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    }
  }
}
