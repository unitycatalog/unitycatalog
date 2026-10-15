package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.utils.Constants;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.Test;

public class IcebergCommitTableTypeExtractorTest {

  private static final IcebergCommitTableTypeExtractor EXTRACTOR =
      new IcebergCommitTableTypeExtractor();

  @Test
  public void managedWhenSetLocationCarriesMarker() {
    UpdateTableRequest request =
        commit(
            new MetadataUpdate.SetLocation(
                "s3://bucket/" + Constants.MANAGED_STORAGE_PREFIX + "/x"));
    assertThat(EXTRACTOR.extract(request)).isEqualTo("MANAGED");
  }

  @Test
  public void externalWhenSetLocationHasNoMarker() {
    UpdateTableRequest request = commit(new MetadataUpdate.SetLocation("s3://bucket/external/x"));
    assertThat(EXTRACTOR.extract(request)).isEqualTo("EXTERNAL");
  }

  @Test
  public void nullWhenNoSetLocation() {
    // A regular commit (no set-location) does not need a table type; the update policy branch
    // ignores #table_type.
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("k", "v"))));
    assertThat(EXTRACTOR.extract(request)).isNull();
  }

  private static UpdateTableRequest commit(MetadataUpdate... updates) {
    return new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertTableDoesNotExist()), List.of(updates));
  }
}
