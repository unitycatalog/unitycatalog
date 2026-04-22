package io.unitycatalog.server.exception;

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/**
 * Base exception handler that normalizes all exceptions to {@link BaseException} before delegating
 * to subclass-specific response formatting via {@link #createErrorResponse(BaseException)}.
 */
public abstract class BaseExceptionHandler implements ExceptionHandlerFunction {

  @Setter
  @Getter(AccessLevel.PROTECTED)
  private static volatile boolean includeStackTrace = false;

  @Override
  public HttpResponse handleException(
      ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    return createErrorResponse(toBaseException(cause));
  }

  protected abstract HttpResponse createErrorResponse(BaseException exception);

  /**
   * Wraps a non-BaseException into a BaseException with an empty stack trace. The empty trace
   * signals {@link #getRelevantStackTrace} to use the original cause's trace instead.
   */
  protected static BaseException wrapException(ErrorCode errorCode, Throwable cause) {
    String message = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
    return wrapException(errorCode, message, cause);
  }

  protected static BaseException wrapException(
      ErrorCode errorCode, String message, Throwable cause) {
    BaseException ex = new BaseException(errorCode, message, cause);
    // Clear the meaningless stack trace so that getRelevantStackTrace will use stack trace of
    // cause instead.
    ex.setStackTrace(new StackTraceElement[0]);
    return ex;
  }

  /**
   * Returns the most useful stack trace for error responses. For direct BaseExceptions (thrown by
   * application code), returns their own trace. For wrappers created by {@link #wrapException}
   * (empty trace), returns the original cause's trace.
   */
  protected static StackTraceElement[] getRelevantStackTrace(BaseException exception) {
    if (exception.getStackTrace().length == 0 && exception.getCause() != null) {
      return exception.getCause().getStackTrace();
    }
    return exception.getStackTrace();
  }

  /**
   * Normalizes any Throwable to a BaseException. Subclasses can override to handle additional
   * exception types (e.g., Iceberg-specific exceptions) before falling back to {@code super}.
   *
   * <p>{@link MalformedRequestBodyException} is the dedicated type for body-parse failures
   * (thrown by {@link TypedErrorJacksonRequestConverter}); its cause is always a {@link
   * JsonProcessingException} whose {@code originalMessage} is surfaced as the wire message.
   * Typed {@code @Param} conversion failures (bad {@code Integer}, bad {@code Boolean}, missing
   * mandatory param, etc.) land in the {@link IllegalArgumentException} branch; their messages
   * are bounded templates with only parameter-name or echoed user-input interpolated.
   */
  protected BaseException toBaseException(Throwable cause) {
    if (cause instanceof BaseException be) {
      return be;
    }
    if (cause instanceof JWTVerificationException) {
      // Use a generic message to avoid leaking JWT verification details to the client
      return wrapException(ErrorCode.UNAUTHENTICATED, "Invalid access token.", cause);
    }
    if (cause instanceof MalformedRequestBodyException ex) {
      JsonProcessingException jpe = ex.getJsonProcessingException();
      String originalMessage = jpe.getOriginalMessage();
      String reason = originalMessage != null ? originalMessage : "invalid JSON";
      return wrapException(ErrorCode.INVALID_ARGUMENT, "Malformed request body: " + reason, cause);
    }
    if (cause instanceof IllegalArgumentException) {
      return wrapException(ErrorCode.INVALID_ARGUMENT, cause);
    }
    return wrapException(ErrorCode.INTERNAL, cause);
  }
}
