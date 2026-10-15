package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.Test;

/**
 * Each case asserts {@code requireAtMostOneSetLocation} and {@code extract} side by side: extract
 * is the count check plus a staged-create gate, so the two agree except that extract drops the
 * location for a regular commit.
 */
public class IcebergCommitLocationExtractorTest {

  private static final IcebergCommitLocationExtractor EXTRACTOR =
      new IcebergCommitLocationExtractor();

  @Test
  public void stagedCreateSurfacesTheSetLocationThroughBoth() {
    UpdateTableRequest request = stagedCreate(new MetadataUpdate.SetLocation("s3://bucket/table"));
    assertThat(IcebergCommitLocationExtractor.requireAtMostOneSetLocation(request))
        .isEqualTo("s3://bucket/table");
    assertThat(EXTRACTOR.extract(request)).isEqualTo("s3://bucket/table");
  }

  @Test
  public void regularCommitSurfacesTheSetLocationOnlyThroughRequireAtMostOne() {
    // requireAtMostOneSetLocation returns the location for any commit; extract gates it to staged
    // creates, so a regular commit resolves no #external_location.
    UpdateTableRequest request = commit(new MetadataUpdate.SetLocation("s3://bucket/table"));
    assertThat(IcebergCommitLocationExtractor.requireAtMostOneSetLocation(request))
        .isEqualTo("s3://bucket/table");
    assertThat(EXTRACTOR.extract(request)).isNull();
  }

  @Test
  public void noSetLocationYieldsNullFromBoth() {
    UpdateTableRequest request = commit();
    assertThat(IcebergCommitLocationExtractor.requireAtMostOneSetLocation(request)).isNull();
    assertThat(EXTRACTOR.extract(request)).isNull();
  }

  @Test
  public void moreThanOneSetLocationIsRejectedByBoth() {
    // The at-most-one rule holds for every commit, so both reject it, even a regular commit.
    UpdateTableRequest request =
        commit(
            new MetadataUpdate.SetLocation("s3://bucket/a"),
            new MetadataUpdate.SetLocation("s3://bucket/b"));
    assertThatThrownBy(() -> IcebergCommitLocationExtractor.requireAtMostOneSetLocation(request))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at most once");
    assertThatThrownBy(() -> EXTRACTOR.extract(request))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("at most once");
  }

  private static UpdateTableRequest commit(MetadataUpdate... updates) {
    return new UpdateTableRequest(List.of(), List.of(updates));
  }

  private static UpdateTableRequest stagedCreate(MetadataUpdate... updates) {
    return new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertTableDoesNotExist()), List.of(updates));
  }
}
