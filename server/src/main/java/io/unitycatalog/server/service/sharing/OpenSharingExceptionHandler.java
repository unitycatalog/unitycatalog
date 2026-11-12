package io.unitycatalog.server.service.sharing;

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.opensharing.http.ApiException;
import io.opensharing.http.ApiExceptionHandler;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import java.util.concurrent.CompletionException;

/**
 * Renders every failure on OpenSharing's provider routes as OpenSharing's {@code {errorCode,
 * message}} body, including UC's own authentication and authorization rejections.
 */
final class OpenSharingExceptionHandler implements ExceptionHandlerFunction {

  private final ApiExceptionHandler openSharing = new ApiExceptionHandler();

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    Throwable failure = cause instanceof CompletionException ? cause.getCause() : cause;
    if (failure instanceof JWTVerificationException) {
      // UC's AuthDecorator throws this for a malformed or unverifiable token.
      failure = ApiException.unauthenticated("Invalid access token.");
    } else if (failure instanceof BaseException uc) {
      if (uc.getErrorCode() == ErrorCode.UNAUTHENTICATED) {
        failure = ApiException.unauthenticated(uc.getErrorMessage());
      } else if (uc.getErrorCode() == ErrorCode.PERMISSION_DENIED) {
        failure = ApiException.permissionDenied(uc.getErrorMessage());
      } else if (uc.getErrorCode() == ErrorCode.NOT_FOUND) {
        // The key mapper's answer for a share or recipient that does not exist.
        failure = ApiException.notFound(uc.getErrorMessage());
      }
    }
    return openSharing.handleException(ctx, req, failure);
  }
}
