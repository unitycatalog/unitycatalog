package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.delta.model.DeltaErrorModel;
import io.unitycatalog.server.delta.model.DeltaErrorResponse;
import java.util.Arrays;

/**
 * Exception handler for the UC Delta API. Converts exceptions to JSON error responses using the
 * generated DeltaErrorResponse/DeltaErrorModel/ErrorType from delta.yaml.
 */
public class DeltaApiExceptionHandler extends BaseExceptionHandler {

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    ErrorCode errorCode = exception.getErrorCode();
    HttpStatus status = errorCode.getDeltaHttpStatus();
    DeltaErrorModel error =
        new DeltaErrorModel()
            .code(status.code())
            .type(errorCode.getDeltaErrorType())
            .message(exception.getErrorMessage())
            .stack(
                isIncludeStackTrace()
                    ? Arrays.stream(getRelevantStackTrace(exception))
                        .map(StackTraceElement::toString)
                        .toList()
                    : null);
    return HttpResponse.ofJson(status, new DeltaErrorResponse().error(error));
  }
}
