package io.unitycatalog.server.sharing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.ResponseHeadersBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.opensharing.http.ApiException;
import io.opensharing.http.ApiFailure;
import io.opensharing.http.ErrorResponse;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;

/**
 * Renders every OpenSharing failure as OpenSharing's {@code {errorCode, message}} body, including
 * UC's own authentication and authorization rejections on OpenSharing's routes.
 */
final class OpenSharingExceptionHandler implements ExceptionHandlerFunction {

  private final ObjectMapper mapper;

  OpenSharingExceptionHandler(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public HttpResponse handleException(
      ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    ApiFailure failure = ApiFailure.of(unwrap(cause));
    ResponseHeadersBuilder headers =
        ResponseHeaders.builder(failure.status()).contentType(MediaType.JSON_UTF_8);
    if (failure.status() == 401) {
      // RFC 9110 requires a 401 to name the expected scheme in WWW-Authenticate.
      headers.add(HttpHeaderNames.WWW_AUTHENTICATE, "Bearer");
    }
    try {
      return HttpResponse.of(
          headers.build(),
          HttpData.wrap(
              mapper.writeValueAsBytes(new ErrorResponse(failure.errorCode(), failure.message()))));
    } catch (JsonProcessingException e) {
      return HttpResponse.ofFailure(e);
    }
  }

  // Armeria reports an unreadable body as an IllegalArgumentException around Jackson's failure.
  private static Exception unwrap(Throwable cause) {
    if (cause instanceof IllegalArgumentException
        && cause.getCause() instanceof JsonProcessingException json) {
      return json;
    }
    if (cause instanceof BaseException uc) {
      if (uc.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
        return ApiException.unauthenticated(uc.getErrorMessage());
      }
      if (uc.getErrorCode() == ErrorCode.PERMISSION_DENIED) {
        return ApiException.permissionDenied(uc.getErrorMessage());
      }
    }
    return cause instanceof Exception e ? e : new RuntimeException(cause);
  }
}
