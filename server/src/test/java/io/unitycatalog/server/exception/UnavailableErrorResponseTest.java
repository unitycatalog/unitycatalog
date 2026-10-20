package io.unitycatalog.server.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import org.junit.jupiter.api.Test;

/** What a client receives for {@link ErrorCode#UNAVAILABLE}, such as a drop that lost the lock. */
public class UnavailableErrorResponseTest {

  private static final BaseException UNAVAILABLE =
      new BaseException(ErrorCode.UNAVAILABLE, "Concurrent modification in progress on table t");

  @Test
  public void icebergClientsGetServiceUnavailable() {
    AggregatedHttpResponse response =
        IcebergRestExceptionHandler.INSTANCE.createErrorResponse(UNAVAILABLE).aggregate().join();

    // The Iceberg client retries a 503 on drop table and then raises ServiceUnavailableException.
    assertThat(response.status().code()).isEqualTo(503);
    assertThat(response.contentUtf8()).contains("\"type\":\"ServiceUnavailableException\"");
  }

  @Test
  public void deltaClientsGetTooManyRequests() {
    AggregatedHttpResponse response =
        DeltaApiExceptionHandler.INSTANCE.createErrorResponse(UNAVAILABLE).aggregate().join();

    // The Delta API has no 503 error type; this is its "retry after a delay" type.
    assertThat(response.status().code()).isEqualTo(429);
    assertThat(response.contentUtf8()).contains("TooManyRequestsException");
  }
}
