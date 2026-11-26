package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DeltaCommit;
import io.unitycatalog.server.model.DeltaUniform;
import io.unitycatalog.server.model.DeltaUniformIceberg;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class DeltaUniformUtilsTest {

  @ParameterizedTest(name = "{0}")
  @CsvSource({
    "2025-01-04T03:13:11.423Z, 1735960391423",
    "2025-01-04T03:13:11.423+00:00, 1735960391423",
    "2025-01-04T03:13:11+05:30, 1735940591000",
    "1000-01-01T00:00:00Z, -30610224000000",
    "9999-12-31T23:59:59.999Z, 253402300799999"
  })
  public void commitTimestampParsesToEpochMillis(String timestamp, long expectedMs) {
    assertThat(
            DeltaUniformUtils.getUniformFields(commitWithTimestamp(timestamp))
                .orElseThrow()
                .convertedDeltaTimestampMs())
        .isEqualTo(expectedMs);
  }

  @ParameterizedTest(name = "''{0}''")
  @ValueSource(
      strings = {
        "2025-01-04T03:13:11.423",
        "not-a-date",
        "",
        " 2025-01-04T03:13:11.423Z",
        "0999-12-31T23:59:59.999Z",
        "+10000-01-01T00:00:00Z",
        "+292278994-08-17T07:12:55.808Z",
        "-292275055-05-16T16:47:04.191Z"
      })
  public void malformedCommitTimestampIsInvalidArgument(String timestamp) {
    assertThatThrownBy(() -> DeltaUniformUtils.getUniformFields(commitWithTimestamp(timestamp)))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessage(
            "uniform.iceberg.converted_delta_timestamp must be an ISO 8601 UTC instant from year"
                + " 1000 to 9999, such as 2025-01-04T03:13:11.423Z, but was '"
                + timestamp
                + "'.");
  }

  @Test
  public void missingCommitTimestampIsRequired() {
    assertThatThrownBy(() -> DeltaUniformUtils.getUniformFields(commitWithTimestamp(null)))
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessage("uniform.iceberg.converted-delta-timestamp is required when uniform is set.");
  }

  private static DeltaCommit commitWithTimestamp(String timestamp) {
    return new DeltaCommit()
        .uniform(
            new DeltaUniform()
                .iceberg(
                    new DeltaUniformIceberg()
                        .metadataLocation(URI.create("s3://bucket/table/metadata/v1.metadata.json"))
                        .convertedDeltaVersion(1L)
                        .convertedDeltaTimestamp(timestamp)));
  }
}
