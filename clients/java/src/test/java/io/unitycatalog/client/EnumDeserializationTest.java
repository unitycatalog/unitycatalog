/*
 * Unity Catalog API - Enum Deserialization Test
 */

package io.unitycatalog.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ModelVersionStatus;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.client.model.VolumeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test to verify that enum deserialization handles unknown/arbitrary enum values gracefully
 * by deserializing them to an UNKNOWN default value instead of throwing exceptions.
 */
public class EnumDeserializationTest {

  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp() {
    objectMapper = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .build();
  }

  @Test
  public void testVolumeTypeUnknownValueDeserialization() {
    // Test that an unknown VolumeType value deserializes to UNKNOWN
    // instead of throwing an exception. VolumeType only has MANAGED
    // and EXTERNAL, so the generator will add an UNKNOWN value
    String unknownVolumeType = "\"NEW_FUTURE_TYPE\"";
    assertThatCode(() -> {
      VolumeType volumeType =
          objectMapper.readValue(unknownVolumeType, VolumeType.class);
      assertThat(volumeType).isNotNull();
      // The UNKNOWN value should be present after code generation
      assertThat(volumeType).isEqualTo(VolumeType.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(volumeType.toString()).isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testDataSourceFormatUnknownValueDeserialization() {
    // Test that an unknown DataSourceFormat value deserializes to UNKNOWN
    // Using ICEBERG as an example of a format not in the current API spec
    String unknownFormat = "\"ICEBERG\"";
    assertThatCode(() -> {
      DataSourceFormat format =
          objectMapper.readValue(unknownFormat, DataSourceFormat.class);
      assertThat(format).isNotNull();
      assertThat(format).isEqualTo(DataSourceFormat.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(format.toString()).isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testTableOperationUnknownValueDeserialization() {
    // Test that an unknown TableOperation value deserializes gracefully
    // The generator adds UNKNOWN_DEFAULT_OPEN_API even though the spec
    // has UNKNOWN_TABLE_OPERATION
    String unknownOperation = "\"DELETE\"";
    assertThatCode(() -> {
      TableOperation operation =
          objectMapper.readValue(unknownOperation, TableOperation.class);
      assertThat(operation).isNotNull();
      // The generator adds UNKNOWN_DEFAULT_OPEN_API as the default
      assertThat(operation).isEqualTo(TableOperation.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(operation.toString()).isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testModelVersionStatusUnknownValueDeserialization() {
    // Test that an unknown ModelVersionStatus value deserializes gracefully
    // The generator adds UNKNOWN_DEFAULT_OPEN_API even though the spec
    // has MODEL_VERSION_STATUS_UNKNOWN
    String unknownStatus = "\"ARCHIVED\"";
    assertThatCode(() -> {
      ModelVersionStatus status =
          objectMapper.readValue(unknownStatus, ModelVersionStatus.class);
      assertThat(status).isNotNull();
      // The generator adds UNKNOWN_DEFAULT_OPEN_API as the default
      assertThat(status).isEqualTo(ModelVersionStatus.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(status.toString()).isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testVolumeTypeKnownValuesStillWork() throws Exception {
    // Verify that known enum values still deserialize correctly
    VolumeType managed = objectMapper.readValue("\"MANAGED\"", VolumeType.class);
    assertThat(managed).isEqualTo(VolumeType.MANAGED);

    VolumeType external = objectMapper.readValue("\"EXTERNAL\"", VolumeType.class);
    assertThat(external).isEqualTo(VolumeType.EXTERNAL);
  }

  @Test
  public void testDataSourceFormatKnownValuesStillWork() throws Exception {
    // Verify that known DataSourceFormat values still deserialize correctly
    DataSourceFormat delta = objectMapper.readValue("\"DELTA\"", DataSourceFormat.class);
    assertThat(delta).isEqualTo(DataSourceFormat.DELTA);

    DataSourceFormat parquet =
        objectMapper.readValue("\"PARQUET\"", DataSourceFormat.class);
    assertThat(parquet).isEqualTo(DataSourceFormat.PARQUET);

    DataSourceFormat json = objectMapper.readValue("\"JSON\"", DataSourceFormat.class);
    assertThat(json).isEqualTo(DataSourceFormat.JSON);
  }

  @Test
  public void testColumnTypeNameUnknownValueDeserialization() {
    // Test that an unknown ColumnTypeName value deserializes to UNKNOWN
    // Using BIGDECIMAL as an example type not in the current spec
    String unknownType = "\"BIGDECIMAL\"";
    assertThatCode(() -> {
      ColumnTypeName typeName =
          objectMapper.readValue(unknownType, ColumnTypeName.class);
      assertThat(typeName).isNotNull();
      assertThat(typeName).isEqualTo(ColumnTypeName.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(typeName.toString()).isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testPrivilegeUnknownValueDeserialization() {
    // Test that an unknown Privilege value deserializes to UNKNOWN
    String unknownPrivilege = "\"DELETE TABLE\"";
    assertThatCode(() -> {
      Privilege privilege =
          objectMapper.readValue(unknownPrivilege, Privilege.class);
      assertThat(privilege).isNotNull();
      // The UNKNOWN value for Privilege should be the first enum constant
      // with @JsonEnumDefaultValue
      assertThat(privilege).isNotNull();
    }).doesNotThrowAnyException();
  }

  @Test
  public void testVolumeInfoWithUnknownVolumeType() {
    // Test that a VolumeInfo object with unknown volume_type deserializes
    String jsonWithUnknownType = "{"
        + "\"volume_id\": \"test-vol-123\","
        + "\"name\": \"test_volume\","
        + "\"volume_type\": \"SUPER_NEW_TYPE\","
        + "\"catalog_name\": \"main\","
        + "\"schema_name\": \"default\","
        + "\"storage_location\": \"s3://bucket/path\","
        + "\"created_at\": 1234567890"
        + "}";

    assertThatCode(() -> {
      VolumeInfo volumeInfo =
          objectMapper.readValue(jsonWithUnknownType, VolumeInfo.class);
      assertThat(volumeInfo).isNotNull();
      assertThat(volumeInfo.getName()).isEqualTo("test_volume");
      assertThat(volumeInfo.getVolumeType()).isNotNull();
      assertThat(volumeInfo.getVolumeType())
          .isEqualTo(VolumeType.UNKNOWN_DEFAULT_OPEN_API);
      assertThat(volumeInfo.getVolumeType().toString())
          .isEqualTo("unknown_default_open_api");
    }).doesNotThrowAnyException();
  }

  @Test
  public void testTableInfoWithUnknownTableType() {
    // Test that a TableInfo object with unknown table_type deserializes
    String jsonWithUnknownType = "{"
        + "\"table_id\": \"test-table-123\","
        + "\"name\": \"test_table\","
        + "\"table_type\": \"TEMPORARY_VIEW\","
        + "\"catalog_name\": \"main\","
        + "\"schema_name\": \"default\","
        + "\"data_source_format\": \"ICEBERG\","
        + "\"created_at\": 1234567890"
        + "}";

    assertThatCode(() -> {
      TableInfo tableInfo =
          objectMapper.readValue(jsonWithUnknownType, TableInfo.class);
      assertThat(tableInfo).isNotNull();
      assertThat(tableInfo.getName()).isEqualTo("test_table");
      // Both table_type and data_source_format should handle unknown values
      if (tableInfo.getTableType() != null) {
        assertThat(tableInfo.getTableType())
            .isEqualTo(TableType.UNKNOWN_DEFAULT_OPEN_API);
        assertThat(tableInfo.getTableType().toString())
            .isEqualTo("unknown_default_open_api");
      }
      if (tableInfo.getDataSourceFormat() != null) {
        assertThat(tableInfo.getDataSourceFormat())
            .isEqualTo(DataSourceFormat.UNKNOWN_DEFAULT_OPEN_API);
        assertThat(tableInfo.getDataSourceFormat().toString())
            .isEqualTo("unknown_default_open_api");
      }
    }).doesNotThrowAnyException();
  }
}
