package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.server.delta.model.ErrorModel;
import io.unitycatalog.server.delta.model.ErrorResponse;
import java.util.Arrays;

/**
 * Exception handler for Delta REST Catalog API. Converts exceptions to JSON error responses using
 * the generated ErrorResponse/ErrorModel/ErrorType from delta.yaml.
 */
public class DeltaRestExceptionHandler extends BaseExceptionHandler {

  @Override
  protected HttpResponse createErrorResponse(BaseException exception) {
    ErrorCode errorCode = exception.getErrorCode();
    HttpStatus status = errorCode.getDeltaHttpStatus();
    ErrorModel error =
        new ErrorModel()
            .code(status.code())
            .type(errorCode.getDeltaErrorType())
            .message(exception.getErrorMessage())
            .stack(
                isIncludeStackTrace()
                    ? Arrays.stream(getRelevantStackTrace(exception))
                        .map(StackTraceElement::toString)
                        .toList()
                    : null);
    return HttpResponse.ofJson(status, new ErrorResponse().error(error));
  }
}
