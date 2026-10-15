package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class IcebergCreateTableTypeExtractorTest {

  private static final IcebergCreateTableTypeExtractor EXTRACTOR =
      new IcebergCreateTableTypeExtractor();
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  public void managedWhenNoLocation() {
    // No location on a create request is a managed table (the server assigns the location).
    assertThat(
            EXTRACTOR.extract(
                CreateTableRequest.builder().withName("t").withSchema(SCHEMA).build()))
        .isEqualTo("MANAGED");
  }

  @Test
  public void externalWhenLocationPresent() {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("t")
            .withSchema(SCHEMA)
            .withLocation("s3://bucket/path")
            .build();
    assertThat(EXTRACTOR.extract(request)).isEqualTo("EXTERNAL");
  }
}
