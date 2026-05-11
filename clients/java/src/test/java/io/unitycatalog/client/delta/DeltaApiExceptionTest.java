package io.unitycatalog.client.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.model.ErrorType;
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
    assertThat(delta.getErrorType()).isEqualTo(ErrorType.NO_SUCH_TABLE_EXCEPTION);
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
    // The public constructor is lenient: parse failures leave error == null rather than throw.
    // Callers wanting a hard parse failure should use from() and react to the empty Optional.
    DeltaApiException delta = new DeltaApiException(new ApiException(400, "msg", null, "not json"));
    assertThat(delta.getError()).isNull();
    assertThat(delta.getErrorCode()).isNull();
    assertThat(delta.getErrorType()).isNull();
    assertThat(delta.getErrorMessage()).isNull();
    // Passthrough still works.
    assertThat(delta.getCode()).isEqualTo(400);
    assertThat(delta.getResponseBody()).isEqualTo("not json");
  }

  @Test
  public void resultIsAlsoAnApiException() {
    // Callers that catch ApiException continue to work after the upgrade.
    Optional<DeltaApiException> upgraded =
        DeltaApiException.from(new ApiException(404, "msg", null, VALID_BODY));
    assertThat(upgraded).isPresent();
    assertThat(upgraded.get()).isInstanceOf(ApiException.class);
  }
}
