package io.unitycatalog.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.Http1HeaderNaming;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.metric.MeterIdPrefixFunction;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.ServerListener;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.VirtualHostBuilder;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.linecorp.armeria.server.annotation.JacksonRequestConverterFunction;
import com.linecorp.armeria.server.annotation.JacksonResponseConverterFunction;
import com.linecorp.armeria.server.annotation.RequestConverterFunction;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import io.micrometer.core.instrument.MeterRegistry;
import io.unitycatalog.server.auth.decorator.AuthorizationGateConverter;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandlingDecorator;
import io.unitycatalog.server.exception.ServiceExceptionHandlingDecorator;
import io.unitycatalog.server.exception.UnroutedIcebergRequestHandler;
import io.unitycatalog.server.service.AuthService;
import io.unitycatalog.server.service.IcebergRestCatalogService;
import io.unitycatalog.server.service.RegisteredService;
import io.unitycatalog.server.service.ScimService;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.service.delta.DeltaApiMappers;
import io.unitycatalog.server.service.delta.DeltaApiService;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import io.unitycatalog.server.utils.ServerProperties;
import java.lang.reflect.ParameterizedType;
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

  /**
   * Relative path the Iceberg REST API is mounted at. Shared with the error handler that renders
   * unrouted Iceberg requests, so the mount point and that handler's prefix cannot drift apart.
   */
  static final String ICEBERG_RELATIVE_PATH = "iceberg";

  private final ServerBuilder armeriaServerBuilder;

  /**
   * Port-based virtual host bound to the API port. The whole API surface -- annotated services, the
   * root banner, the docs, and the auth decorators -- is registered here, so it is served only on
   * the API port. Armeria's default virtual host is served on every bound port; leaving it empty
   * and scoping each surface to its own port-based virtual host is what keeps the API off the
   * observability port and the observability endpoints off the API port.
   */
  private final VirtualHostBuilder apiVirtualHost;

  /**
   * Port-based virtual host bound to the dedicated observability port. {@code /livez}, {@code
   * /readyz}, and {@code /metrics} are registered here (via {@link #observabilityService}), so they
   * are served only on that port. It is a second port on the same {@link Server}, never a separate
   * server, so both listeners share one event loop and fail together.
   */
  private final VirtualHostBuilder observabilityVirtualHost;

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
      int port,
      int observabilityPort,
      String basePath,
      String controlPath,
      ServerProperties serverProperties) {
    // The API and observability endpoints are two ports on one server, isolated by being on
    // separate port-based virtual hosts. If the two ports were equal, both virtual hosts would bind
    // the same port and the surfaces would collapse onto one listener -- putting /metrics and the
    // probes back on the serving interface. Fail fast rather than silently weaken the isolation.
    if (observabilityPort == port) {
      throw new IllegalArgumentException(
          String.format(
              "server.observability.port (%d) must differ from the API port (%d): they are two"
                  + " ports on the same server, and sharing one port would collapse the API and the"
                  + " observability endpoints onto a single listener.",
              observabilityPort, port));
    }
    this.armeriaServerBuilder =
        Server.builder()
            // The API port binds the loopback interfaces only: clients reach it through the
            // in-process URL transcoder, never directly.
            .localPort(port, SessionProtocol.HTTP)
            // Second port on the SAME server (not a separate Server) for the observability
            // endpoints. Both listeners share one JVM and event loop, so they fail together --
            // there is no state where the obs port is healthy while the API port is not -- while
            // keeping /metrics and the probes off the main API listener. Unlike the API port this
            // binds all interfaces, because kubelet and Prometheus reach it at the pod IP (not
            // loopback); restrict it with network policy.
            .port(observabilityPort, SessionProtocol.HTTP)
            // Armeria names HTTP/1 headers in their lowercase HTTP/2 form by default. Released
            // Iceberg clients read our response headers out of a plain map keyed by the name as
            // received, so a header they look up by its traditional spelling -- "ETag" for a
            // conditional loadTable -- is invisible to them unless we write it that way.
            .http1HeaderNaming(Http1HeaderNaming.traditional());
    // The API surface lives on a port-based virtual host bound to the API port, and the
    // observability endpoints on one bound to the observability port. The default virtual host is
    // left empty; since it is otherwise served on every bound port, scoping each surface to its own
    // port-based virtual host is what makes the API answer only on the API port and the probes and
    // /metrics only on the observability port.
    this.apiVirtualHost = armeriaServerBuilder.virtualHost(port);
    this.apiVirtualHost.serviceUnder("/docs", new DocService());
    this.apiVirtualHost.service("/", (ctx, req) -> HttpResponse.of("Hello, Unity Catalog!"));
    this.observabilityVirtualHost = armeriaServerBuilder.virtualHost(observabilityPort);
    this.basePath = basePath;
    this.controlPath = controlPath;
    // Renders the 404s and 405s Armeria answers before a service is reached as Iceberg error
    // documents, for the Iceberg API's paths only. Server-level, so it applies to unrouted requests
    // on any virtual host.
    this.armeriaServerBuilder.errorHandler(
        new UnroutedIcebergRequestHandler(basePath + ICEBERG_RELATIVE_PATH + "/"));
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
    // Attached to the API virtual host, where the API services they guard live.
    for (DecoratingHttpServiceFunction decorator : List.of(accessDecorator, authDecorator)) {
      apiVirtualHost.routeDecorator().pathPrefix(basePath).build(decorator);
      apiVirtualHost
          .routeDecorator()
          .pathPrefix(controlPath)
          .exclude(controlPath + "auth/tokens")
          .build(decorator);
    }

    // On the API virtual host so it wraps the route decorators above and renders what they throw
    // (an auth failure as 401/403, not 500). It must be here rather than server-level: with a
    // server-level GlobalExceptionHandlingDecorator the access-control tests observed auth failures
    // rendering as 500 -- it did not wrap this port-based virtual host's route decorators. This
    // instance carries no dialect: it finds the per-service one for the matched route.
    apiVirtualHost.decorator(GlobalExceptionHandlingDecorator::new);
    return this;
  }

  /**
   * Registers an unauthenticated observability service (a health probe or the metrics scrape) on
   * the dedicated observability port only. Because it is bound to the observability virtual host,
   * it answers on that port and is 404 on the API port, keeping metrics and probes off the serving
   * interface.
   */
  ArmeriaServerBuilder observabilityService(String path, HttpService service) {
    observabilityVirtualHost.service(path, service);
    return this;
  }

  /**
   * Sets the Micrometer registry Armeria records into and installs a decorator on the API virtual
   * host that emits per-endpoint request count / latency / error metrics under the {@code
   * http.server} prefix. On the API virtual host so it meters API traffic (wrapping outside the
   * auth decorators, as it did when the whole surface was on the default virtual host); the
   * observability port's own scrapes are not counted as http.server traffic.
   */
  ArmeriaServerBuilder meterRegistry(MeterRegistry meterRegistry) {
    armeriaServerBuilder.meterRegistry(meterRegistry);
    apiVirtualHost.decorator(
        MetricCollectingService.newDecorator(MeterIdPrefixFunction.ofDefault("http.server")));
    return this;
  }

  /** Registers a server lifecycle listener (used to start/stop background probes). */
  ArmeriaServerBuilder serverListener(ServerListener listener) {
    armeriaServerBuilder.serverListener(listener);
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
          // The Jackson converter leaks its own error text (exception class, mapped types, source
          // position) on any surface. Only on the Iceberg surface is an unreadable body rewritten
          // into a clean message, since its REST spec fixes the error contract clients parse; the
          // other dialects still surface the raw converter message.
          case ICEBERG -> new MalformedBodyRejectingConverter(bodyConverter(icebergMapper));
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
    apiVirtualHost
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

  /**
   * Reports a body the Jackson converter cannot read as an Iceberg bad request. Jackson's own
   * failure names the exception, the Java types the body was being mapped onto, and the reader's
   * location in it, none of which belongs in an error a client is shown; and the converter rethrows
   * it as an {@link IllegalArgumentException}, a name an Iceberg client does not know.
   *
   * <p>Recognizing the failure here rather than in the exception handler keeps it to request
   * bodies: the same Jackson failures are raised when the server reads JSON of its own, such as a
   * table's metadata file, and those have nothing to do with the body the caller sent.
   */
  private static final class MalformedBodyRejectingConverter implements RequestConverterFunction {

    private final RequestConverterFunction delegate;

    private MalformedBodyRejectingConverter(RequestConverterFunction delegate) {
      this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public Object convertRequest(
        ServiceRequestContext ctx,
        AggregatedHttpRequest request,
        Class<?> expectedResultType,
        ParameterizedType expectedParameterizedResultType)
        throws Exception {
      try {
        return delegate.convertRequest(
            ctx, request, expectedResultType, expectedParameterizedResultType);
      } catch (IllegalArgumentException | JsonProcessingException failure) {
        // Armeria's Jackson converter rethrows an unreadable body as IllegalArgumentException with
        // the Jackson failure as its cause; a converter that throws that failure directly is caught
        // by the JsonProcessingException arm. Everything else, including the FallthroughException
        // Armeria uses to say a converter does not handle this parameter, is never caught and
        // propagates untouched.
        Throwable reason =
            failure instanceof JsonProcessingException ? failure : failure.getCause();
        if (reason instanceof JsonParseException) {
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "Malformed request body: not valid JSON");
        }
        if (reason instanceof MismatchedInputException) {
          // A body that is not there raises the same failure as one shaped wrong ("No content to
          // map due to end-of-input"), and telling that caller the structure is wrong sends them
          // to their schema when what they sent was nothing.
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT,
              request.content().isEmpty()
                  ? "Malformed request body: no content"
                  : "Malformed request body: not the structure this endpoint accepts");
        }
        throw failure;
      }
    }
  }
}
