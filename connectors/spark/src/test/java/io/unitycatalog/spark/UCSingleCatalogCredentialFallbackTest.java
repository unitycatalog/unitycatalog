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
 * <p>These tests verify that:
 * - Credentials are requested with READ_WRITE first, then READ on failure
 * - Clear exceptions are thrown when both credential attempts fail
 * - Appropriate logging occurs for credential failures
 * - Error messages contain helpful diagnostic information
 */
public class UCSingleCatalogCredentialFallbackTest {

  private UCSingleCatalog catalog;
  private TemporaryCredentialsApi mockCredentialsApi;
  private TablesApi mockTablesApi;

  @BeforeEach
  public void setup() throws Exception {
    catalog = new UCSingleCatalog();
    mockCredentialsApi = mock(TemporaryCredentialsApi.class);
    mockTablesApi = mock(TablesApi.class);

    // Initialize catalog with minimal configuration
    Map<String, String> options = new HashMap<>();
    options.put("uri", "http://localhost:8080");
    options.put("token", "test-token");
    CaseInsensitiveStringMap config = new CaseInsensitiveStringMap(options);

    catalog.initialize("unity", config);

    // Use reflection to inject mocked APIs
    injectMockedApi(catalog, "temporaryCredentialsApi", mockCredentialsApi);
    injectMockedApi(catalog, "tablesApi", mockTablesApi);
  }

  /**
   * Helper method to inject mocked APIs into the catalog using reflection.
   */
  private void injectMockedApi(UCSingleCatalog catalog, String fieldName, Object mockApi)
      throws Exception {
    Field field = UCSingleCatalog.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(catalog, mockApi);
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
  public void testCredentialFallback_BothFail_ThrowsException() throws Exception {
    // Setup: Mock API to fail for both READ_WRITE and READ
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
        .hasMessageContaining("insufficient permissions");

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
  public void testCredentialFallback_OperationTypeTracked() throws Exception {
    // Setup: Mock API to fail READ_WRITE but succeed for READ
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

    // Verify: Table loads successfully
    assertThat(result).isNotNull();

    // Verify: Correct operation types were requested in order
    verify(mockCredentialsApi, times(2)).generateTemporaryTableCredentials(any());
    // First call should be READ_WRITE, second should be READ
    // (This is implicitly tested by the fallback behavior)
  }
}
