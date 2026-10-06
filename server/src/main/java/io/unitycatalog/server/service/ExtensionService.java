package io.unitycatalog.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An annotated service that an embedded extension, such as OpenSharing, mounts at its own absolute
 * path. It speaks its own protocol, so it brings its own JSON mapper and error dialect. Like UC's
 * own services, it is authenticated by UC's security decorators and authorized by its methods'
 * {@code @AuthorizeExpression}s.
 */
public interface ExtensionService extends RegisteredService {

  /** Reads request bodies and writes response bodies. */
  ObjectMapper objectMapper();
}
