package io.unitycatalog.server.service;

import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.unitycatalog.server.exception.GlobalExceptionHandler;

/**
 * Implemented by the Unity Catalog REST services mounted under the main API base path: catalogs,
 * schemas, tables, volumes, functions, models, credentials, and the rest.
 *
 * <p>Supplies the Unity Catalog error envelope for all of them, and names the group in the type
 * system so {@code ArmeriaServerBuilder.annotate} selects the right protocol and cannot be handed a
 * SCIM service by mistake.
 */
public interface UnityCatalogRestService extends RegisteredService {

  @Override
  default ExceptionHandlerFunction exceptionHandler() {
    return GlobalExceptionHandler.INSTANCE;
  }
}
