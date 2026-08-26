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
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import com.linecorp.armeria.server.docs.DocService;
import io.unitycatalog.server.auth.decorator.AuthorizationGateConverter;
import io.unitycatalog.server.exception.GlobalExceptionHandlingDecorator;
import io.unitycatalog.server.exception.ServiceExceptionHandlingDecorator;
import io.unitycatalog.server.service.AuthService;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.RegisteredService;
import io.unitycatalog.server.service.ScimService;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.service.delta.DeltaApiMappers;
import io.unitycatalog.server.service.delta.DeltaApiService;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.List;
import java.util.Objects;

/**
 * Wraps Armeria's {@link ServerBuilder} with Unity-Catalog-aware registration. Callers register
 * each annotated service with {@code annotate}, overloaded per service type so the argument picks
 * the {@link ServiceProtocol} and with it the base path, body mapper, and response converter. The
 * error dialect is not chosen here: the service supplies its own via {@link
 * RegisteredService#exceptionHandler()}. Those overloads funnel through the single private {@link
 * #register}, the only place this class registers an annotated service, so every route is wired the
 * same way, including the PAYLOAD-source gate in front of body binding. {@link
 * #withSecurityDecorators} attaches caller-supplied access/auth decorators to the API path
 * prefixes, and {@link #build()} builds the server.
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

  /**
   * Whether to install the PAYLOAD-source authorization gate in front of body binding. Tied to the
   * same flag that installs the access decorator the gate depends on, so the gate is never left
   * waiting on an authorizer that nothing will produce.
   */
  private final boolean authorizationEnabled;

  // Body mappers and response converters, created once and reused across registrations. Only the
  // body mapper and the (optional) response converter vary by protocol; see bodyConverter for how
  // the request converter is chosen.
  private final ObjectMapper ucMapper;
  private final JacksonResponseConverterFunction scimResponseConverter;
  private final ObjectMapper icebergMapper;
  private final JacksonResponseConverterFunction icebergResponseConverter;
  private final ObjectMapper deltaMapper;
  private final JacksonResponseConverterFunction deltaResponseConverter;

  ArmeriaServerBuilder(
      int port, String basePath, String controlPath, ServerProperties serverProperties) {
    this.armeriaServerBuilder =
        Server.builder()
            .localPort(port, SessionProtocol.HTTP)
            .serviceUnder("/docs", new DocService());
    this.armeriaServerBuilder.service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"));
    this.basePath = basePath;
    this.controlPath = controlPath;
    this.authorizationEnabled = serverProperties.isAuthorizationEnabled();
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
  ArmeriaServerBuilder annotate(String relativePath, AuthService service) {
    register(ServiceProtocol.AUTH, relativePath, service);
    return this;
  }

  /** Registers a SCIM2 service (UC body + SCIM response converter) under the control path. */
  ArmeriaServerBuilder annotate(String relativePath, ScimService service) {
    register(ServiceProtocol.SCIM, relativePath, service);
    return this;
  }

  /** Registers a standard Unity Catalog REST service at {@code basePath + relativePath}. */
  ArmeriaServerBuilder annotate(String relativePath, UnityCatalogRestService service) {
    register(ServiceProtocol.UC, relativePath, service);
    return this;
  }

  /** Registers an Iceberg REST catalog service (Iceberg mapper) under the base path. */
  ArmeriaServerBuilder annotate(String relativePath, IcebergRestCatalogService service) {
    register(ServiceProtocol.ICEBERG, relativePath, service);
    return this;
  }

  /** Registers a UC Delta REST service (Delta mapper) under the base path. */
  ArmeriaServerBuilder annotate(String relativePath, DeltaApiService service) {
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
      DecoratingHttpServiceFunction accessDecorator, DecoratingHttpServiceFunction authDecorator) {
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

    // Also registered globally, where it is outermost and can catch what the route decorators above
    // throw. This instance carries no dialect: it finds the per-service one for the matched route.
    armeriaServerBuilder.decorator(GlobalExceptionHandlingDecorator::new);
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
   * The single registration point behind every {@code annotate} overload, and the only place this
   * class calls {@code annotatedService}. It selects the protocol-specific body and response
   * converters and registers the service at {@code basePath + relativePath} ({@code ""} mounts at
   * the base path root). Because this is the sole registration path and every arm gets its body
   * converter from {@link #bodyConverter}, no annotated service can reach the server ungated while
   * authorization is enabled.
   *
   * <p>The per-protocol converter selection is a switch expression with no default, so it is
   * checked for exhaustiveness: adding a {@link ServiceProtocol} constant without a corresponding
   * arm is a compile error, and a new service kind cannot be registered without also deciding its
   * converters.
   */
  private void register(ServiceProtocol protocol, String relativePath, RegisteredService service) {
    RequestConverterFunction requestConverter =
        switch (protocol) {
          case AUTH, UC, SCIM -> bodyConverter(ucMapper);
          case ICEBERG -> bodyConverter(icebergMapper);
          case DELTA -> bodyConverter(deltaMapper);
        };
    // Auth and UC services have no response converter; they return HttpResponse.ofJson directly.
    List<JacksonResponseConverterFunction> responseConverters =
        switch (protocol) {
          case AUTH, UC -> List.of();
          case SCIM -> List.of(scimResponseConverter);
          case ICEBERG -> List.of(icebergResponseConverter);
          case DELTA -> List.of(deltaResponseConverter);
        };
    // The service names its own dialect, so both paths use the same value: exceptionHandlers()
    // covers exceptions thrown inside the handler, the per-service decorator covers those thrown by
    // decorators sitting outside it.
    ExceptionHandlerFunction handler = service.exceptionHandler();
    armeriaServerBuilder
        .annotatedService()
        .pathPrefix(protocol.basePath(basePath, controlPath) + relativePath)
        .requestConverters(requestConverter)
        .responseConverters(responseConverters)
        .exceptionHandlers(handler)
        .decorator(delegate -> new ServiceExceptionHandlingDecorator(delegate, handler))
        .build(service);
  }

  /**
   * The request converter for a service's body parameters: the authorization gate wrapping Jackson
   * when authorization is enabled, plain Jackson when it is not. With authorization disabled
   * nothing produces a {@code PayloadAuthorizer}, so a gate would wait on a value never produced.
   */
  private RequestConverterFunction bodyConverter(ObjectMapper mapper) {
    JacksonRequestConverterFunction jackson = new JacksonRequestConverterFunction(mapper);
    return authorizationEnabled ? new AuthorizationGateConverter(jackson, mapper) : jackson;
  }
}
