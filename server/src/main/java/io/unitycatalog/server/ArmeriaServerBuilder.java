package io.unitycatalog.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import com.linecorp.armeria.server.docs.DocService;
import io.unitycatalog.server.auth.decorator.AuthorizationGateConverter;
import io.unitycatalog.server.exception.ExceptionHandlingDecorator;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.service.AuthService;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.delta.DeltaApiMappers;
import io.unitycatalog.server.service.delta.DeltaApiService;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import java.util.List;
import java.util.Objects;

/**
 * Wraps Armeria's {@link ServerBuilder} with Unity-Catalog-aware registration. Callers register
 * each annotated service through a protocol-specific {@code annotate*} method ({@link
 * #annotateUc}, {@link #annotateAuth}, {@link #annotateScim}, {@link #annotateIceberg},
 * {@link #annotateDelta}). Those methods funnel through the single private {@link #register}, the
 * only place this class registers an annotated service, and it always installs the PAYLOAD-source
 * authorization gate in front of the body converter -- so no annotated service can reach the server
 * ungated. {@link #withSecurityDecorators} attaches caller-supplied access/auth decorators to the
 * API path prefixes, and {@link #build()} builds the server.
 *
 * <p>{@link UnityCatalogServer} bootstraps the collaborators (Hibernate, authorizer, repositories),
 * constructs the service handlers, and decides whether authorization is enabled; this class turns
 * registration + assembly into a running {@link Server} and does not itself know about server
 * properties or security context.
 */
public class ArmeriaServerBuilder {

  private final ServerBuilder armeriaServerBuilder;
  private final String basePath;
  private final String controlPath;

  // Body mappers and response converters, created once and reused across registrations. The
  // request converter is always the gated wrapper (see gatedJackson); only the wrapped mapper and
  // the response converter vary by protocol.
  private final ObjectMapper ucMapper;
  private final JacksonResponseConverterFunction scimResponseConverter;
  private final ObjectMapper icebergMapper;
  private final JacksonResponseConverterFunction icebergResponseConverter;
  private final ObjectMapper deltaMapper;
  private final JacksonResponseConverterFunction deltaResponseConverter;

  ArmeriaServerBuilder(int port, String basePath, String controlPath) {
    this.armeriaServerBuilder =
        Server.builder()
            .localPort(port, SessionProtocol.HTTP)
            .serviceUnder("/docs", new DocService());
    this.armeriaServerBuilder.service(
        "/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"));
    this.basePath = basePath;
    this.controlPath = controlPath;
    this.ucMapper =
        JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    this.scimResponseConverter =
        new JacksonResponseConverterFunction(
            JsonMapper.builder()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .build());
    this.icebergMapper = IcebergObjectMapper.mapper();
    this.icebergResponseConverter = new JacksonResponseConverterFunction(icebergMapper);
    this.deltaMapper = DeltaApiMappers.MAPPER;
    this.deltaResponseConverter = new JacksonResponseConverterFunction(deltaMapper);
  }

  /** Registers a control-plane auth service at {@code controlPath + relativePath}. */
  ArmeriaServerBuilder annotateAuth(String relativePath, AuthService service) {
    register(ServiceProtocol.AUTH, relativePath, service);
    return this;
  }

  // TODO: introduce an interface for the two SCIM services and replace Object here with it.
  /** Registers a SCIM2 service (UC body + SCIM response converter) under the control path. */
  ArmeriaServerBuilder annotateScim(String relativePath, Object service) {
    register(ServiceProtocol.SCIM, relativePath, service);
    return this;
  }

  // TODO: introduce an interface for all UC REST services and replace Object here with it.
  /** Registers a standard Unity Catalog REST service at {@code basePath + relativePath}. */
  ArmeriaServerBuilder annotateUc(String relativePath, Object service) {
    register(ServiceProtocol.UC, relativePath, service);
    return this;
  }

  /** Registers an Iceberg REST catalog service (Iceberg mapper) under the base path. */
  ArmeriaServerBuilder annotateIceberg(String relativePath, IcebergRestCatalogService service) {
    register(ServiceProtocol.ICEBERG, relativePath, service);
    return this;
  }

  /** Registers a UC Delta REST service (Delta mapper) under the base path. */
  ArmeriaServerBuilder annotateDelta(String relativePath, DeltaApiService service) {
    register(ServiceProtocol.DELTA, relativePath, service);
    return this;
  }

  /**
   * Wires the access and authentication decorators onto the API path prefixes. The caller owns the
   * decision of whether to enable authorization (and simply skips calling this when it is
   * disabled); this method only knows how to attach the decorators it is given -- path prefixes,
   * the {@code auth/tokens} exclusion, and that the exception handler must sit at the bottom of the
   * chain.
   *
   * <p>Both decorators are attached to the same path prefixes. Armeria runs decorators in the
   * reverse of their registration order, so {@code authDecorator} (authentication) runs before
   * {@code accessDecorator} (authorization) at request time -- the opposite of the parameter order.
   *
   * @param accessDecorator authorization decorator; runs second at request time
   * @param authDecorator authentication decorator; runs first at request time
   */
  ArmeriaServerBuilder withSecurityDecorators(
      DecoratingHttpServiceFunction accessDecorator,
      DecoratingHttpServiceFunction authDecorator) {
    Objects.requireNonNull(accessDecorator, "accessDecorator");
    Objects.requireNonNull(authDecorator, "authDecorator");
    for (DecoratingHttpServiceFunction decorator : List.of(accessDecorator, authDecorator)) {
      armeriaServerBuilder.routeDecorator().pathPrefix(basePath).build(decorator);
      armeriaServerBuilder
          .routeDecorator()
          .pathPrefix(controlPath)
          .exclude(controlPath + "auth/tokens")
          .build(decorator);
    }

    armeriaServerBuilder.decorator(new ExceptionHandlingDecorator(new GlobalExceptionHandler()));
    return this;
  }

  /** Builds the Armeria {@link Server}. */
  Server build() {
    return armeriaServerBuilder.build();
  }

  /**
   * The wire protocol an annotated service speaks: which path family it mounts under (control-plane
   * vs. the main API base path) plus, in {@link #register}, its body and response converters.
   */
  private enum ServiceProtocol {
    AUTH(true),
    SCIM(true),
    UC(false),
    ICEBERG(false),
    DELTA(false);

    private final boolean underControlPath;

    ServiceProtocol(boolean underControlPath) {
      this.underControlPath = underControlPath;
    }

    /** Resolves this protocol's base path against the caller-supplied path prefixes. */
    String basePath(String basePath, String controlPath) {
      return underControlPath ? controlPath : basePath;
    }
  }

  /**
   * The single registration point behind every {@code annotate*} method, and the only place this
   * class calls {@code annotatedService}. It selects the protocol-specific body mapper and response
   * converter, wraps the request converter with {@link AuthorizationGateConverter} (so the
   * PAYLOAD-source authorization gate fires before body binding for every route without exception),
   * and registers the service at {@code basePath + relativePath} ({@code ""} mounts at the base
   * path root). Because this is the sole registration path and it always wraps with the gate, no
   * annotated service can reach the server ungated.
   *
   * <p>The per-protocol converter selection is a switch expression with no default, so it is
   * checked for exhaustiveness: adding a {@link ServiceProtocol} constant without a corresponding
   * arm is a compile error, and a new service kind cannot be registered without also deciding its
   * (still gated) converters.
   */
  private void register(ServiceProtocol protocol, String relativePath, Object service) {
    List<Object> converters = switch (protocol) {
      // Auth and UC services do not have response converter. They return HttpResponse.ofJson
      // directly.
      case AUTH, UC -> List.of(gatedJackson(ucMapper));
      case SCIM -> List.of(gatedJackson(ucMapper), scimResponseConverter);
      case ICEBERG -> List.of(gatedJackson(icebergMapper), icebergResponseConverter);
      case DELTA -> List.of(gatedJackson(deltaMapper), deltaResponseConverter);
    };
    armeriaServerBuilder.annotatedService(
        protocol.basePath(basePath, controlPath) + relativePath, service, converters.toArray());
  }

  /**
   * Wraps a Jackson body converter with {@link AuthorizationGateConverter}. Every annotated
   * service's request converter goes through this (via {@link #register}) so the PAYLOAD-source
   * auth check fires before body binding.
   */
  private static RequestConverterFunction gatedJackson(ObjectMapper mapper) {
    return new AuthorizationGateConverter(new JacksonRequestConverterFunction(mapper));
  }
}
