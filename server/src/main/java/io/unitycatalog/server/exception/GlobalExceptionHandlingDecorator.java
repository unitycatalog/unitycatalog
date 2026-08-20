package io.unitycatalog.server.exception;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

/**
 * Registered globally, making it the outermost element of every chain and so the only thing that
 * can catch a denial thrown by a route decorator, before the service is reached at all.
 *
 * <p>It tries to delegate to the {@link ServiceExceptionHandlingDecorator} of the calling service,
 * or falls back to the default {@link GlobalExceptionHandler} if it finds none.
 */
public final class GlobalExceptionHandlingDecorator extends ExceptionHandlingDecorator {

  public GlobalExceptionHandlingDecorator(HttpService delegate) {
    super(delegate);
  }

  @Override
  protected ExceptionHandlerFunction resolveHandler(ServiceRequestContext ctx) {
    ServiceExceptionHandlingDecorator service =
        ctx.config().service().as(ServiceExceptionHandlingDecorator.class);
    // Absent for routes registered without a service dialect: the "/" greeting and the doc service.
    return service != null ? service.handler() : GlobalExceptionHandler.INSTANCE;
  }
}
