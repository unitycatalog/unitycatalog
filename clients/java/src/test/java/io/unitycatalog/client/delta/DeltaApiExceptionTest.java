package io.unitycatalog.client.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DeltaApiException}: parsing the Delta error envelope, the lenient {@link
 * DeltaApiException#from} factory's empty/present return values, and field passthrough from the
 * wrapped {@link ApiException}.
 */
public class DeltaApiExceptionTest {

  private static final String VALID_BODY =
      "{\"error\":{\"code\":404,\"type\":\"NoSuchTableException\","
          + "\"message\":\"Table not found: c.s.t\"}}";

  @Test
  public void fromParsesValidErrorEnvelope() {
    ApiException source = new ApiException(404, "msg", null, VALID_BODY);
    DeltaApiException delta = DeltaApiException.from(source).orElseThrow();
    assertThat(delta.getErrorCode()).isEqualTo(404);
    assertThat(delta.getErrorType()).isEqualTo(DeltaErrorType.NO_SUCH_TABLE_EXCEPTION);
    assertThat(delta.getErrorMessage()).isEqualTo("Table not found: c.s.t");
    // ApiException fields pass through unchanged.
    assertThat(delta.getCode()).isEqualTo(404);
    assertThat(delta.getResponseBody()).isEqualTo(VALID_BODY);
  }

  @Test
  public void fromReturnsEmptyOnNullBody() {
    ApiException source = new ApiException(500, "msg", null, null);
    assertThat(DeltaApiException.from(source)).isEmpty();
  }

  @Test
  public void fromReturnsEmptyOnMalformedJson() {
    ApiException source = new ApiException(400, "msg", null, "this is not json");
    assertThat(DeltaApiException.from(source)).isEmpty();
  }

  @Test
  public void fromReturnsEmptyWhenErrorEnvelopeAbsent() {
    // Well-formed JSON, but no "error" key -- the body isn't a Delta error response.
    ApiException source = new ApiException(400, "msg", null, "{\"unrelated\":\"x\"}");
    assertThat(DeltaApiException.from(source)).isEmpty();
  }

  @Test
  public void constructorTolerantOfUnparseableBody() {
    // The public constructor is lenient: parse failures leave error == empty rather than throw.
    Optional<DeltaApiException> delta =
        DeltaApiException.from(new ApiException(400, "msg", null, "not json"));
    assertThat(delta).isEmpty();
  }

  @Test
  public void resultIsAlsoAnApiException() {
    // Callers that catch ApiException continue to work after the upgrade..
    DeltaApiException upgraded =
        DeltaApiException.from(
                new ApiException("msg", new RuntimeException("re"), 404, null, VALID_BODY))
            .get();
    assertThat(upgraded).isInstanceOf(ApiException.class);
    assertThat(upgraded.getErrorCode()).isEqualTo(404);
    // The cause is the original ApiException
    assertThat(upgraded.getCause()).isInstanceOf(ApiException.class);
    ApiException cause = (ApiException) upgraded.getCause();
    // The cause.cause is the original cause RuntimeException
    assertThat(cause.getCause()).isInstanceOf(RuntimeException.class);
  }

  @Test
  public void fromReturnsEmptyWhenErrorFieldIsNull() {
    // `{"error":null}` -- the envelope is well-formed but carries no error block. Treated the
    // same as a missing envelope: from() returns empty so callers fall back to the raw
    // ApiException.
    ApiException source = new ApiException(500, "msg", null, "{\"error\":null}");
    assertThat(DeltaApiException.from(source)).isEmpty();
  }

  @Test
  public void parsesEmptyErrorBlockWithNullSubfields() {
    // `{"error":{}}` -- the error block is present but every subfield is null. By design we return
    // a present DeltaApiException (the envelope was parseable) with null typed accessors; callers
    // wanting a "no useful info" signal should null-check the accessors. Locks the best-effort
    // contract described in the from() Javadoc.
    DeltaApiException delta =
        DeltaApiException.from(new ApiException(400, "msg", null, "{\"error\":{}}")).orElseThrow();
    assertThat(delta.getErrorCode()).isNull();
    assertThat(delta.getErrorType()).isNull();
    assertThat(delta.getErrorMessage()).isNull();
    assertThat(delta.getError()).isNotNull();
  }

  @Test
  public void tolerantOfUnknownFields() {
    // Pins the FAIL_ON_UNKNOWN_PROPERTIES=false setting in createObjectMapper(). If someone
    // removes that configure() call, this test catches the regression: a server emitting a new
    // spec field would otherwise stop parsing entirely.
    String body =
        "{\"error\":{\"code\":404,\"type\":\"NoSuchTableException\","
            + "\"message\":\"Table not found\",\"unknownField\":\"ignored\"}}";
    DeltaApiException delta =
        DeltaApiException.from(new ApiException(404, "msg", null, body)).orElseThrow();
    assertThat(delta.getErrorCode()).isEqualTo(404);
    assertThat(delta.getErrorType()).isEqualTo(DeltaErrorType.NO_SUCH_TABLE_EXCEPTION);
    assertThat(delta.getErrorMessage()).isEqualTo("Table not found");
  }
}
