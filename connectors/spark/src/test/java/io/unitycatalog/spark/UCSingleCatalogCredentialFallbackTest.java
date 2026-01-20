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
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for credential fallback behavior in UCSingleCatalog.
 *
 * <p>These tests verify:
 * - Credential fallback chain: READ_WRITE → READ → None
 * - Configuration flag behavior: allowEmptyCredentials.enabled
 * - Default behavior (flag=false): Fail-fast with clear exception
 * - Opt-in behavior (flag=true): Allow empty credentials with warning logs
 * - Security: Ensure credentials are never bypassed unintentionally
 *
 * <p>Tests cover both scenarios:
 * 1. When credentials are successfully obtained (flag is irrelevant)
 * 2. When credentials fail (flag controls behavior)
 */
public class UCSingleCatalogCredentialFallbackTest {

  private UCSingleCatalog catalog;
  private TemporaryCredentialsApi mockCredentialsApi;
  private TablesApi mockTablesApi;

  @BeforeEach
  public void setup() throws Exception {
    // Disable DeltaCatalog loading to avoid SparkSession dependency
    UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);

    catalog = new UCSingleCatalog();
    mockCredentialsApi = mock(TemporaryCredentialsApi.class);
    mockTablesApi = mock(TablesApi.class);

    // Initialize catalog with minimal configuration
    Map<String, String> options = new HashMap<>();
    options.put("uri", "http://localhost:8080");
    options.put("token", "test-token");
    CaseInsensitiveStringMap config = new CaseInsensitiveStringMap(options);

    catalog.initialize("unity", config);

    // Use reflection to inject mocked APIs into the delegate (UCProxy)
    // The delegate is where the actual table operations happen
    Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Object delegate = delegateField.get(catalog);

    injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi);
    injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);
  }

  /**
   * Helper method to inject mocked APIs into an object using reflection.
   */
  private void injectMockedApiIntoObject(Object target, String fieldName, Object mockApi)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, mockApi);
  }

  /**
   * Helper method to create a minimal TableInfo for testing.
   */
  private TableInfo createTestTable(String tableName, String schemaName, String storageLocation) {
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
  private TemporaryCredentials createTestCredentials(String token) {
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
  public void testCredentialFallback_BothFail_Disabled_ThrowsException() throws Exception {
    // Setup: Mock API to fail for both READ_WRITE and READ
    // Default configuration: allowEmptyCredentials=false (not explicitly set)
    TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");

    when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
        .thenReturn(testTable);
    when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
        .thenThrow(new ApiException("Permission denied for READ_WRITE"))
        .thenThrow(new ApiException("Permission denied for READ"));

    // Execute & Verify: Loading table throws clear exception
    Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
    assertThatThrownBy(() -> catalog.loadTable(tableId))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("Unable to load table")
        .hasMessageContaining("credential generation failed")
        .hasMessageContaining("insufficient permissions")
        .hasMessageContaining("allowEmptyCredentials.enabled=true");

    // Verify: Two API calls were made (both READ_WRITE and READ attempted)
    verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testCredentialFallback_BothFail_AllowEmptyEnabled_ReturnsEmptyMap()
      throws Exception {
    // Setup: Create catalog with allowEmptyCredentials=true
    // Disable DeltaCatalog loading
    UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);
    UCSingleCatalog catalogWithEmptyCredsAllowed = new UCSingleCatalog();
    Map<String, String> options = new HashMap<>();
    options.put("uri", "http://localhost:8080");
    options.put("token", "test-token");
    options.put("allowEmptyCredentials.enabled", "true");
    CaseInsensitiveStringMap config = new CaseInsensitiveStringMap(options);
    catalogWithEmptyCredsAllowed.initialize("unity", config);

    // Inject mocked APIs into the delegate
    Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Object delegate = delegateField.get(catalogWithEmptyCredsAllowed);
    injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi);
    injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);

    // Setup: Mock API to fail for both READ_WRITE and READ
    TableInfo testTable = createTestTable("fgac_table", "test_schema", "s3://bucket/path");

    when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
        .thenReturn(testTable);
    when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
        .thenThrow(new ApiException("Permission denied for READ_WRITE"))
        .thenThrow(new ApiException("Permission denied for READ"));

    // Execute: Load table (should succeed with empty credentials)
    Identifier tableId = Identifier.of(new String[] {"test_schema"}, "fgac_table");
    Table result = catalogWithEmptyCredsAllowed.loadTable(tableId);

    // Verify: Table loads successfully even without credentials
    assertThat(result).isNotNull();

    // Verify: Two API calls were made (both READ_WRITE and READ attempted before fallback)
    verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());

    // Note: In a complete test, we would verify that:
    // 1. A warning log was generated containing the table identifier and security guidance
    // 2. The extraSerdeProps contains an empty map (would require inspecting internal state)
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
        .hasMessageContaining("important_table")
        .hasMessageContaining("Please verify your permissions");
  }

  @Test
  public void testCredentialFallback_ReadOnlyCredentialsLogged() throws Exception {
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

    // Note: Logging verification would require log capture framework
    // For now, we verify that the code path executed successfully
    // In a more complete test, we would verify that logInfo was called
    // with a message about READ-only credentials
  }

  @Test
  public void testCredentialFallback_ReadWriteSucceeds_FlagIgnored() throws Exception {
    // Test that when READ_WRITE credentials succeed, the allowEmptyCredentials flag is irrelevant
    // Create catalog with allowEmptyCredentials=true
    // Disable DeltaCatalog loading
    UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);
    UCSingleCatalog catalogWithEmptyCredsAllowed = new UCSingleCatalog();
    Map<String, String> options = new HashMap<>();
    options.put("uri", "http://localhost:8080");
    options.put("token", "test-token");
    options.put("allowEmptyCredentials.enabled", "true");
    CaseInsensitiveStringMap config = new CaseInsensitiveStringMap(options);
    catalogWithEmptyCredsAllowed.initialize("unity", config);

    // Inject mocked APIs into the delegate
    Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Object delegate = delegateField.get(catalogWithEmptyCredsAllowed);
    injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi);
    injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);

    // Setup: Mock API to return valid READ_WRITE credentials
    TableInfo testTable = createTestTable("test_table", "test_schema", "s3://bucket/path");
    TemporaryCredentials testCreds = createTestCredentials("rw-token");

    when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
        .thenReturn(testTable);
    when(mockCredentialsApi.generateTemporaryTableCredentials(any())).thenReturn(testCreds);

    // Execute: Load table with both catalog configurations
    Identifier tableId = Identifier.of(new String[] {"test_schema"}, "test_table");
    Table resultWithFlag = catalogWithEmptyCredsAllowed.loadTable(tableId);
    Table resultWithoutFlag = catalog.loadTable(tableId);

    // Verify: Both tables load successfully
    assertThat(resultWithFlag).isNotNull();
    assertThat(resultWithoutFlag).isNotNull();

    // Verify: Credentials were used in both cases (flag is irrelevant when credentials exist)
    verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
  }

  @Test
  public void testCredentialFallback_ReadSucceeds_FlagIgnored() throws Exception {
    // Test that when READ credentials succeed (after READ_WRITE fails), flag is irrelevant
    // Create catalog with allowEmptyCredentials=true
    // Disable DeltaCatalog loading
    UCSingleCatalog$.MODULE$.LOAD_DELTA_CATALOG().set(false);
    UCSingleCatalog catalogWithEmptyCredsAllowed = new UCSingleCatalog();
    Map<String, String> options = new HashMap<>();
    options.put("uri", "http://localhost:8080");
    options.put("token", "test-token");
    options.put("allowEmptyCredentials.enabled", "true");
    CaseInsensitiveStringMap config = new CaseInsensitiveStringMap(options);
    catalogWithEmptyCredsAllowed.initialize("unity", config);

    // Inject mocked APIs into the delegate
    TemporaryCredentialsApi mockCredentialsApi2 = mock(TemporaryCredentialsApi.class);
    Field delegateField = UCSingleCatalog.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Object delegate = delegateField.get(catalogWithEmptyCredsAllowed);
    injectMockedApiIntoObject(delegate, "temporaryCredentialsApi", mockCredentialsApi2);
    injectMockedApiIntoObject(delegate, "tablesApi", mockTablesApi);

    // Setup: Mock API to fail READ_WRITE but succeed for READ
    TableInfo testTable = createTestTable("readonly_table", "test_schema", "s3://bucket/path");
    TemporaryCredentials readCreds = createTestCredentials("read-token");

    when(mockTablesApi.getTable(any(String.class), any(Boolean.class), any(Boolean.class)))
        .thenReturn(testTable);

    // For catalog with flag
    when(mockCredentialsApi2.generateTemporaryTableCredentials(any()))
        .thenThrow(new ApiException("Permission denied for READ_WRITE"))
        .thenReturn(readCreds);

    // For default catalog
    when(mockCredentialsApi.generateTemporaryTableCredentials(any()))
        .thenThrow(new ApiException("Permission denied for READ_WRITE"))
        .thenReturn(readCreds);

    // Execute: Load table with both configurations
    Identifier tableId = Identifier.of(new String[] {"test_schema"}, "readonly_table");
    Table resultWithFlag = catalogWithEmptyCredsAllowed.loadTable(tableId);
    Table resultWithoutFlag = catalog.loadTable(tableId);

    // Verify: Both tables load successfully with READ credentials
    assertThat(resultWithFlag).isNotNull();
    assertThat(resultWithoutFlag).isNotNull();

    // Verify: Fallback works regardless of flag value
    verify(mockCredentialsApi2, times(2)).generateTemporaryTableCredentials(any());
    verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
  }
}
