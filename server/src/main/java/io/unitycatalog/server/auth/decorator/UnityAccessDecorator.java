package io.unitycatalog.server.auth.decorator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.internal.server.annotation.AnnotatedService;
import com.linecorp.armeria.server.DecoratingHttpServiceFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingHttpService;
import io.netty.util.AttributeKey;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeResourceKey;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import io.unitycatalog.server.persist.utils.ExternalLocationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.unitycatalog.server.auth.decorator.AuthorizeKeyLocator.Source.PARAM;
import static io.unitycatalog.server.auth.decorator.AuthorizeKeyLocator.Source.PAYLOAD;
import static io.unitycatalog.server.auth.decorator.AuthorizeKeyLocator.Source.SYSTEM;

/**
 * Armeria access control Decorator.
 *
 * <p>This decorator provides the ability to protect Armeria service methods with per method access
 * control rules. This decorator is used in conjunction with following 3 annotations to define
 * authorization rules and identify requests parameters for objects to authorize with:
 *
 * <p>1. {@code @AuthorizeExpression} - This defines a Spring Expression Language expression to
 * evaluate to make an authorization decision.
 *
 * <p>2. {@code @AuthorizeResourceKey} - This annotation is used to define request and payload
 * parameters for the authorization context. These are typically things like catalog, schema and
 * table names. This annotation may be used at both the method and method parameter context. It may
 * be specified more than once per method to map parameters to object keys.
 *
 * <p>3. {@code @AuthorizeKey} - This annotation is used to define request and payload parameters
 * for that are usually not resources but parameters of the operation. For example, table_type,
 * operation, etc.
 */
public class UnityAccessDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessDecorator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final KeyMapper keyMapper;
  private final UserRepository userRepository;

  private final UnityAccessEvaluator evaluator;

  // Context attribute key for passing ResultFilter to service methods
  public static final AttributeKey<ResultFilter> RESULT_FILTER_ATTR =
      AttributeKey.valueOf(ResultFilter.class, "RESULT_FILTER");

  public UnityAccessDecorator(UnityCatalogAuthorizer authorizer, Repositories repositories)
      throws BaseException {
    try {
      evaluator = new UnityAccessEvaluator(authorizer);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error initializing access evaluator.", e);
    }
    keyMapper = repositories.getKeyMapper();
    userRepository = repositories.getUserRepository();
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
      throws Exception {
    LOGGER.debug("AccessDecorator checking {}", req.path());

    Method method = findServiceMethod(ctx.config().service());
    if (method == null) {
      throw new RuntimeException("Couldn't unwrap service.");
    }

    // Find the authorization parameters to use for this service method.
    String expression = findAuthorizeExpression(method);
    List<AuthorizeKeyLocator> locators = findAuthorizeKeys(method);

    // Check for response filtering annotation
    Optional<ResponseAuthorizeFilter> filterAnnotation =
        Optional.ofNullable(method.getAnnotation(ResponseAuthorizeFilter.class));

    if (expression == null) {
      throw new RuntimeException("No authorization expression found.");
    }
    if (!locators.isEmpty()) {
      UUID principal = userRepository.findPrincipalId();
      return authorizeByRequest(
          delegate, ctx, method, req, principal, locators, expression, filterAnnotation);
    } else {
      LOGGER.warn("No authorization resource(s) found.");
      // going to assume the expression is just #deny or #permit
    }

    return delegate.serve(ctx, req);
  }

  private HttpResponse authorizeByRequest(
      HttpService delegate,
      ServiceRequestContext ctx,
      Method method,
      HttpRequest req,
      UUID principal,
      List<AuthorizeKeyLocator> locators,
      String expression,
      Optional<ResponseAuthorizeFilter> filterAnnotation) throws Exception {
    //
    // Based on the query and payload parameters defined on the service method (that
    // have been gathered as Locators), we'll attempt to find the entity/resource that
    // we want to authorize against.

    Map<SecurableType, Object> resourceKeys = new HashMap<>();
    Map<String, Object> nonResourceValues = new HashMap<>();

    // Split up the locators by type, because we have to extract the value from the request
    // different ways for different types

    List<AuthorizeKeyLocator> systemLocators = locators.stream()
        .filter(l -> l.getSource().equals(SYSTEM))
        .toList();
    List<AuthorizeKeyLocator> paramLocators = locators.stream()
        .filter(l -> l.getSource().equals(PARAM))
        .toList();
    List<AuthorizeKeyLocator> payloadLocators = locators.stream()
        .filter(l -> l.getSource().equals(PAYLOAD))
        .toList();

    // Add system-type keys, just metastore for now.
    systemLocators.forEach(l -> resourceKeys.put(l.getType().get(), "metastore"));

    // Extract the query/path parameter values just by grabbing them from the request
    paramLocators.forEach(l -> {
      String value = ctx.pathParam(l.getKey()) != null
          ? ctx.pathParam(l.getKey())
          : ctx.queryParam(l.getKey());
      if (l.getType().isPresent()) {
        resourceKeys.put(l.getType().get(), value);
      } else {
        nonResourceValues.put(l.getVariableName(), value);
      }
    });

    AbstractEvaluationAction evaluateAction =
        filterAnnotation
            .<AbstractEvaluationAction>map(x -> new ResultFilterAction(ctx, method))
            .orElseGet(() -> new RequestEvaluationAction(method));

    if (payloadLocators.isEmpty()) {
      Map<SecurableType, UUID> resourceIds = mapResourceKeys(resourceKeys, nonResourceValues);
      evaluateAction.beforeRequest(principal, expression, resourceIds, nonResourceValues);
    } else {
      // Since we have PAYLOAD locators, we can only interrogate the payload while the request
      // is being evaluated, via peekData()
      LOGGER.debug("Checking authorization before in peekData.");

      PeekDataHandler peekDataHandler = new PeekDataHandler(
          req.contentType(),
          payloadLocators,
          resourceKeys,
          nonResourceValues);

      // Note that peekData only gets called for requests that actually have data (like PUT&POST)

      req = req.peekData(data -> {
        // This code block is not called immediately. It is called later as part of delegate.serve()
        LOGGER.debug("Authorization peekData invoked.");

        peekDataHandler.processPeekData(data);
        Map<SecurableType, UUID> resourceIds = mapResourceKeys(resourceKeys, nonResourceValues);
        evaluateAction.beforeRequest(principal, expression, resourceIds, nonResourceValues);
      });
    }

    HttpResponse response = delegate.serve(ctx, req);
    return evaluateAction.afterRequest(response);
  }

  private static Object findPayloadValue(String key, Map<String, Object> payload) {
    // TODO: investigate better object traversal functionality
    String[] args = key.split("[.]", 2);
    if (args.length == 1) {
      return payload.get(args[0]);
    } else {
      if (payload.get(args[0]) instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) payload.get(args[0]);
        return findPayloadValue(args[1], value);
      } else {
        return null;
      }
    }
  }

  private abstract static class AbstractEvaluationAction {
    private boolean beforeRequestWasCalled = false;
    protected Method method;
    protected static final String ERR_AUTH_NOT_EXECUTED =
        "Authorization required but failed to execute";

    AbstractEvaluationAction(Method method) {
      this.method = method;
    }

    void beforeRequest(
        UUID principal,
        String expression,
        Map<SecurableType, UUID> resourceIds,
        Map<String, Object> nonResourceValues) {
      beforeRequestWasCalled = true;
      evaluateBeforeRequest(principal, expression, resourceIds, nonResourceValues);
    }

    abstract void evaluateBeforeRequest(
        UUID principal,
        String expression,
        Map<SecurableType, UUID> resourceIds,
        Map<String, Object> nonResourceValues);

    HttpResponse afterRequest(HttpResponse response) {
      return HttpResponse.of(response.aggregate().thenApply(
        aggregated -> {
          if (aggregated.status().isSuccess()) {
            if (!beforeRequestWasCalled) {
              LOGGER.error(
                  "SECURITY VIOLATION: Method {} did not execute evaluateAction",
                  method.getName());
              throw new BaseException(ErrorCode.PERMISSION_DENIED, ERR_AUTH_NOT_EXECUTED);
            }
            evaluateAfterRequest();
          }
          return aggregated.toHttpResponse();
        }));
    }

    abstract void evaluateAfterRequest();
  }

  private class RequestEvaluationAction extends AbstractEvaluationAction {

    RequestEvaluationAction(Method method) {
      super(method);
    }

    @Override
    public void evaluateBeforeRequest(
        UUID principal,
        String expression,
        Map<SecurableType, UUID> resourceIds,
        Map<String, Object> nonResourceValues) {
      if (!evaluator.evaluate(principal, expression, resourceIds, nonResourceValues)) {
        throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
      }
    }

    @Override
    public void evaluateAfterRequest() {}
  }

  private class ResultFilterAction extends AbstractEvaluationAction {
    private final ServiceRequestContext ctx;
    private ResultFilter resultFilter = null;

    ResultFilterAction(ServiceRequestContext ctx, Method method) {
      super(method);
      this.ctx = ctx;
    }

    @Override
    public void evaluateBeforeRequest(
        UUID principal,
        String expression,
        Map<SecurableType, UUID> resourceIds,
        Map<String, Object> nonResourceValues) {
      resultFilter =
          new ResultFilter(
              evaluator, principal, expression, resourceIds, nonResourceValues, keyMapper);
      ctx.setAttr(RESULT_FILTER_ATTR, resultFilter);
    }

    @Override
    public void evaluateAfterRequest() {
      if (!resultFilter.wasCalled()) {
        LOGGER.error(
            "SECURITY VIOLATION: Method {} with @ResponseAuthorizeFilter did not call "
                + "applyResponseFilter(). This is a security vulnerability!",
            method.getName());
        throw new BaseException(ErrorCode.PERMISSION_DENIED, ERR_AUTH_NOT_EXECUTED);
      }
    }
  }

  private Map<SecurableType, UUID> mapResourceKeys(
      Map<SecurableType, Object> resourceKeys, Map<String, Object> nonResourceValues) {
    Map<SecurableType, UUID> resourceIds = keyMapper.mapResourceKeys(resourceKeys);

    if (resourceKeys.containsKey(SecurableType.EXTERNAL_LOCATION)) {
      // KeyMapper will try to map a path of EXTERNAL_LOCATION to any data securable if the path
      // belongs to it, instead of just the external location. This new variable is introduced so
      // that auth expression can easily check if the input path overlaps with any data securable.
      boolean noOverlapWithDataSecurable =
          ExternalLocationUtils.DATA_SECURABLE_TYPES.stream()
              .allMatch(type -> resourceIds.get(type) == null);
      nonResourceValues.put("no_overlap_with_data_securable", noOverlapWithDataSecurable);
    }

    if (!resourceIds.keySet().containsAll(resourceKeys.keySet())) {
      LOGGER.warn("Some resource keys have unresolved ids.");
    }

    LOGGER.debug("resourceIds = {}", resourceIds);
    return resourceIds;
  }

  private static String findAuthorizeExpression(Method method) {
    // TODO: Cache this by method

    AuthorizeExpression annotation = method.getAnnotation(AuthorizeExpression.class);

    if (annotation != null) {
      LOGGER.debug("authorize expression = {}", annotation.value());
      return annotation.value();
    } else {
      LOGGER.debug("authorize = (none found)");
      return null;
    }
  }

  private static List<AuthorizeKeyLocator> findAuthorizeKeys(Method method) {
    // TODO: Cache this by method

    List<AuthorizeKeyLocator> locators = new ArrayList<>();

    AuthorizeResourceKey methodKey = method.getAnnotation(AuthorizeResourceKey.class);

    // If resource is on the method, its source is from a global/system variable
    if (methodKey != null) {
      locators.add(
          AuthorizeKeyLocator.builder()
              .source(SYSTEM)
              .type(Optional.of(methodKey.value()))
              .build());
    }

    for (Parameter parameter : method.getParameters()) {
      AuthorizeResourceKey[] allResourceKeys =
          parameter.getAnnotationsByType(AuthorizeResourceKey.class);
      for (AuthorizeResourceKey key : allResourceKeys) {
        locators.add(AuthorizeKeyLocator.from(key, parameter));
      }

      AuthorizeKey[] allNonResourceKeys = parameter.getAnnotationsByType(AuthorizeKey.class);
      for (AuthorizeKey key : allNonResourceKeys) {
        locators.add(AuthorizeKeyLocator.from(key, parameter));
      }
    }
    return locators;
  }

  private static Method findServiceMethod(HttpService httpService) throws ClassNotFoundException {
    if (httpService.unwrap() instanceof SimpleDecoratingHttpService decoratingService
        && decoratingService.unwrap() instanceof AnnotatedService service) {

      LOGGER.debug("serviceName = {}, methodName = {}",
          service.serviceName(),
          service.methodName());

      Class<?> clazz = Class.forName(service.serviceName());
      List<Method> methods = findMethodsByName(clazz, service.methodName());
      return (methods.size() == 1) ? methods.get(0) : null;
    } else {
      return null;
    }
  }

  private static List<Method> findMethodsByName(Class<?> clazz, String methodName) {
    List<Method> matchingMethods = new ArrayList<>();
    Method[] methods = clazz.getDeclaredMethods();

    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        matchingMethods.add(method);
      }
    }

    return matchingMethods;
  }

  private static class PeekDataHandler {
    // This is a little ugly - peekData provides only a block of data at a time, so lets
    // buffer it up until we think its complete. A better long term solution would be to
    // abandon this method and either integrate with Spring Boot to get full AOP support
    // with aspect weaving, or build implement custom aspect weaving so we can intercept
    // the method call directly and extract the payload data from the method arguments.

    private final MediaType contentType;
    private final List<AuthorizeKeyLocator> payloadLocators;
    private final Map<SecurableType, Object> resourceKeys;
    private final Map<String, Object> nonResourceValues;
    private final ByteArrayOutputStream dataStream = new ByteArrayOutputStream();

    private PeekDataHandler(
        MediaType contentType,
        List<AuthorizeKeyLocator> payloadLocators,
        Map<SecurableType, Object> resourceKeys,
        Map<String, Object> nonResourceValues) {
      this.contentType = contentType;
      this.payloadLocators = payloadLocators;
      this.resourceKeys = resourceKeys;
      this.nonResourceValues = nonResourceValues;
    }

    private boolean processPeekData(HttpData data) {
      // TODO: For now, we're going to assume JSON data, but might need to support other
      // content types.
      if (contentType.equals(MediaType.JSON)) {
        try {
          dataStream.write(data.array());
          LOGGER.debug("Payload: {}", dataStream.toString());
        } catch (IOException e) {
          // IGNORE
        }

        // Unfortunately we don't appear to get a signal that we've got all the data, so we have to
        // resort to attempting to parse the data whenever it _looks_ complete.
        // TODO: try to optimize this using Jackson streaming or something else.
        if (data.array()[data.array().length - 1] == '}') {
          try {
            Map<String, Object> payload = MAPPER.readValue(
                dataStream.toByteArray(),
                new TypeReference<>() {
                });

            payloadLocators.forEach(
                l -> {
                  Object value = findPayloadValue(l.getKey(), payload);
                  if (l.getType().isPresent()) {
                    resourceKeys.put(l.getType().get(), value);
                  } else {
                    if (value != null && value.getClass().isEnum()) {
                      // Convert enums to their string representation
                      value = value.toString();
                    }
                    nonResourceValues.put(l.getVariableName(), value);
                  }
                });

            return true;
          } catch (IOException e) {
            // This is probably because we read partial data.
            LOGGER.warn("Error parsing payload: {}", e.getMessage());
          }
        }

      } else {
        LOGGER.warn("Skipping content-type: {}", contentType);
      }

      return false;
    }
  }
}
