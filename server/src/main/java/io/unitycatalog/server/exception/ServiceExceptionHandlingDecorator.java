package io.unitycatalog.server.exception;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import java.util.Objects;

/** Registered around a single service, and the one place a service's error dialect is recorded. */
public final class ServiceExceptionHandlingDecorator extends ExceptionHandlingDecorator {

  private final ExceptionHandlerFunction handler;

  public ServiceExceptionHandlingDecorator(HttpService delegate, ExceptionHandlerFunction handler) {
    super(delegate);
    this.handler = Objects.requireNonNull(handler, "handler");
  }

  /** The dialect of the service this wraps. */
  public ExceptionHandlerFunction handler() {
    return handler;
  }

  @Override
  protected ExceptionHandlerFunction resolveHandler(ServiceRequestContext ctx) {
    return handler;
  }
}
