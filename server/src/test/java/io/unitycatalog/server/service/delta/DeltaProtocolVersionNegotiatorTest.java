package io.unitycatalog.server.service.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class DeltaProtocolVersionNegotiatorTest {

  @ParameterizedTest(name = "client={0} -> {1}")
  @CsvSource({
    "1.0, 1.0",
    // Client supports 1.0-1.5; server's 1.0 is the highest in common.
    "1.5, 1.0",
    // Multiple majors; only major 1 overlaps.
    "'1.0,2.3', 1.0",
    "'2.3,1.2', 1.0",
    // Whitespace around entries is tolerated.
    "' 1.0 , 2.0 ', 1.0",
    // Duplicate majors take the highest minor.
    "'1.0,1.7', 1.0",
    // A non-overlapping entry alongside an overlapping one does not break negotiation.
    "'0.9,1.0', 1.0",
  })
  public void negotiatesHighestMutuallySupportedVersion(String client, String expected) {
    assertThat(DeltaProtocolVersionNegotiator.negotiate(client)).isEqualTo(expected);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"   "})
  public void rejectsMissingParameter(String client) {
    assertInvalidArgument(client, "Must supply the protocol-versions parameter");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1",
        "1.",
        ".0",
        "v1.0",
        "1.0.0",
        "abc",
        "1.0,",
        ",1.0",
        "1.0,,2.0",
        "1.x",
        // Oversized digit runs must be rejected as malformed, not overflow Integer.parseInt
        // into a 500.
        "99999999999999.0",
        "1.999999999999"
      })
  public void rejectsMalformedEntries(String client) {
    assertInvalidArgument(client, "is not of the form <major>.<minor>");
  }

  @ParameterizedTest
  @ValueSource(strings = {"2.0", "0.9", "0.0", "2.0,3.1"})
  public void rejectsWhenNoVersionIsShared(String client) {
    assertInvalidArgument(client, "No mutually supported protocol version");
  }

  private static void assertInvalidArgument(String client, String messageSubstring) {
    assertThatThrownBy(() -> DeltaProtocolVersionNegotiator.negotiate(client))
        .isInstanceOf(BaseException.class)
        .satisfies(
            e ->
                assertThat(((BaseException) e).getErrorCode())
                    .isEqualTo(ErrorCode.INVALID_ARGUMENT))
        .hasMessageContaining(messageSubstring)
        // Every rejection names the versions the server supports so the client can adapt.
        .hasMessageContaining("Server supports protocol versions: 1.0.");
  }
}
