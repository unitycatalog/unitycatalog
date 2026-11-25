package io.unitycatalog.server.service;

import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.exception.ScimExceptionHandler;

/**
 * Implemented by the SCIM2 services mounted under the control path: {@code scim2/Users} and {@code
 * scim2/Me}.
 *
 * <p>SCIM differs from the plain REST services in serialization and in error format: these routes
 * get a SCIM response converter, and errors are SCIM Errors (RFC 7644 section 3.12) instead of the
 * Unity Catalog envelope. This type is what {@code ArmeriaServerBuilder.annotate} matches, so a
 * service is only mounted where its converter and error handler are applied.
 */
public interface ScimService extends RegisteredService {

  @Override
  default ExceptionHandlerFunction exceptionHandler() {
    return ScimExceptionHandler.INSTANCE;
  }
}
