package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

/**
 * Catch exceptions and send response
 *
 * <p>The normal ExceptionHandler annotations do not catch exceptions from Decorators. This can be
 * used to catch those and return the desired response.
 *
 * <p>This should be set at the bottom of the decorator chain to ensure that it will catch
 * exceptions from upstream decorators.
 *
 * <p>.decorator(decorator1) .decorator(decorator2) .decorator(new ExceptionHandlingDecorator())
 */
public class ExceptionHandlingDecorator implements DecoratingHttpServiceFunction {

  private final ExceptionHandlerFunction exceptionHandlerFunction;

  public ExceptionHandlingDecorator(ExceptionHandlerFunction exceptionHandlerFunction) {
    this.exceptionHandlerFunction = exceptionHandlerFunction;
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    try {
      return delegate.serve(ctx, req);
    } catch (Exception e) {
      return exceptionHandlerFunction.handleException(ctx, req, e);
    }
  }
}
