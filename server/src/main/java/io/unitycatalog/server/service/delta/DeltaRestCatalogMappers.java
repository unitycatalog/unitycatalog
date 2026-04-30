package io.unitycatalog.server.service.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.unitycatalog.server.delta.serde.DeltaTypeModule;

/**
 * Shared Jackson configuration for Delta REST Catalog endpoints.
 *
 * <p>The same mapper is used both by the server's request / response converters (see {@code
 * UnityCatalogServer#addDeltaApiServices}) and by tests that want to pin the exact wire-format the
 * server produces. Centralizing it here means a change to the Delta response shape -- e.g. adding a
 * serialization feature -- takes effect in one place, and regression tests that use the same mapper
 * can't silently drift from production behavior.
 *
 * <p>The returned {@link ObjectMapper} is fully configured: {@link JsonInclude.Include#NON_NULL} so
 * absent cloud-specific keys are omitted from the wire, plus {@link DeltaTypeModule} for the custom
 * (de)serializers of polymorphic Delta types. Callers must treat the instance as effectively
 * immutable -- do not register additional modules or change inclusion policies on it.
 */
public final class DeltaRestCatalogMappers {

  private DeltaRestCatalogMappers() {}

  /** The mapper used by the Delta REST Catalog request and response converters. */
  public static final ObjectMapper MAPPER = buildMapper();

  private static ObjectMapper buildMapper() {
    ObjectMapper mapper =
        JsonMapper.builder().serializationInclusion(JsonInclude.Include.NON_NULL).build();
    mapper.registerModule(new DeltaTypeModule());
    return mapper;
  }
}
