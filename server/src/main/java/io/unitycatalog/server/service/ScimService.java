package io.unitycatalog.server.service;

import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.exception.GlobalExceptionHandler;

/**
 * Implemented by the SCIM2 services mounted under the control path: {@code scim2/Users} and {@code
 * scim2/Me}.
 *
 * <p>SCIM differs from the plain REST services in serialization, not in error format: those routes
 * get a SCIM response converter, but errors still use the Unity Catalog envelope. This type is what
 * {@code ArmeriaServerBuilder.annotate} matches, so a service is only mounted where its converter
 * is applied.
 */
public interface ScimService extends RegisteredService {

  @Override
  default ExceptionHandlerFunction exceptionHandler() {
    return GlobalExceptionHandler.INSTANCE;
  }
}
