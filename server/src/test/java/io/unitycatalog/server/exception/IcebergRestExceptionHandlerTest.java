package io.unitycatalog.server.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.junit.jupiter.api.Test;

public class IcebergRestExceptionHandlerTest {

  private static AggregatedHttpResponse render(ErrorCode code) {
    return IcebergRestExceptionHandler.INSTANCE
        .createErrorResponse(new BaseException(code, "boom"))
        .aggregate()
        .join();
  }

  /**
   * A commit whose outcome is unknown must reach an Iceberg client as {@code
   * CommitStateUnknownException} (HTTP 500), so the client does not treat the commit as failed and
   * rebase. {@code INTERNAL}, which shares the 500 status, must stay a plain {@code
   * ServiceFailureException} -- i.e. the two are mapped distinctly.
   */
  @Test
  public void commitStateUnknownMapsToIcebergCommitStateUnknown() {
    AggregatedHttpResponse unknown = render(ErrorCode.COMMIT_STATE_UNKNOWN);
    assertThat(unknown.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(unknown.contentUtf8()).contains("CommitStateUnknownException");

    AggregatedHttpResponse internal = render(ErrorCode.INTERNAL);
    assertThat(internal.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(internal.contentUtf8()).contains("ServiceFailureException");
  }
}
