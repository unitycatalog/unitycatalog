package io.unitycatalog.server.service;

import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

/**
 * Implemented by every annotated service that can be registered with the server. Its one job is to
 * name the error dialect the service speaks, so a failure is rendered the same way whether it was
 * thrown in the handler or in a decorator ahead of it.
 *
 * <p>An interface rather than a base class, so a service's protocol and its choice of
 * implementation base stay independent: it can extend {@link AuthorizedService} for the
 * authorization helpers while declaring its protocol here.
 *
 * <p>Deliberately left without a default. Each protocol supplies the dialect its services share
 * ({@link UnityCatalogRestService}, {@link ScimService}), so a new protocol has to decide what it
 * speaks rather than silently inheriting the Unity Catalog envelope.
 *
 * <p>The dialect is declared in code rather than as an {@code @ExceptionHandler} annotation on each
 * service: {@code ArmeriaServerBuilder} passes it to Armeria at registration time and the
 * exception-handling decorators read it back, so both paths are driven by this one method and
 * cannot disagree.
 */
public interface RegisteredService {

  /** The dialect errors from this service are rendered in. */
  ExceptionHandlerFunction exceptionHandler();
}
