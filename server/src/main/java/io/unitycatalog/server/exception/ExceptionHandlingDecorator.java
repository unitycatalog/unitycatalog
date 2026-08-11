package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

/**
 * Turns an exception into a formatted error response, in the dialect of the service that was
 * called.
 *
 * <p>Armeria's own {@code @ExceptionHandler} support only wraps the annotated-service invocation,
 * while authentication and PARAM/SYSTEM-source authorization run in route decorators outside it. An
 * exception can therefore surface at two depths, so there are two subclasses registered at two
 * depths: {@link ServiceExceptionHandlingDecorator} per service, and {@link
 * GlobalExceptionHandlingDecorator} outermost. They differ only in how they answer {@link
 * #resolveHandler}.
 *
 * <p>The global one must be registered last so it ends up at the bottom of the decorator chain,
 * where it can catch exceptions from the decorators above it.
 */
public abstract class ExceptionHandlingDecorator extends SimpleDecoratingHttpService {

  protected ExceptionHandlingDecorator(HttpService delegate) {
    super(delegate);
  }

  /** The dialect to render an exception from this request in. */
  protected abstract ExceptionHandlerFunction resolveHandler(ServiceRequestContext ctx);

  @Override
  public final HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) throws Exception {
    try {
      return unwrap().serve(ctx, req);
    } catch (Exception e) {
      return resolveHandler(ctx).handleException(ctx, req, e);
    }
  }
}
