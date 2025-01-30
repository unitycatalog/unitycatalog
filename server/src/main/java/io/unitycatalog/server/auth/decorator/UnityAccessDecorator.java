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
import com.linecorp.armeria.server.annotation.Param;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.AuthorizeKeys;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.PARAM;
import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.PAYLOAD;
import static io.unitycatalog.server.auth.decorator.KeyLocator.Source.SYSTEM;

/**
 * Armeria access control Decorator.
 * <p>
 * This decorator provides the ability to protect Armeria service methods with per method access
 * control rules. This decorator is used in conjunction with two annotations, @AuthorizeExpression
 * and @AuthorizeKey to define authorization rules and identify requests parameters for objects
 * to authorize with.
 *
 * {@code @AuthorizeExpression} - This defines a Spring Expression Language expression to evaluate to make
 * an authorization decision.
 * {@code @AuthorizeKey} - This annotation is used to define request and payload parameters for the authorization
 * context. These are typically things like catalog, schema and table names. This annotation may be used
 * at both the method and method parameter context. It may be specified more than once per method to
 * map parameters to object keys.
 */
public class UnityAccessDecorator implements DecoratingHttpServiceFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnityAccessDecorator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final KeyMapper keyMapper;
  private final UserRepository userRepository;

  private final UnityAccessEvaluator evaluator;

  public UnityAccessDecorator(UnityCatalogAuthorizer authorizer, Repositories repositories) throws BaseException {
    try {
      evaluator = new UnityAccessEvaluator(authorizer);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new BaseException(ErrorCode.INTERNAL, "Error initializing access evaluator.", e);
    }
    keyMapper = new KeyMapper(repositories);
    userRepository = repositories.getUserRepository();
  }

  @Override
  public HttpResponse serve(HttpService delegate, ServiceRequestContext ctx, HttpRequest req)
          throws Exception {
    LOGGER.debug("AccessDecorator checking {}", req.path());

    Method method = findServiceMethod(ctx.config().service());

    if (method != null) {

      // Find the authorization parameters to use for this service method.
      String expression = findAuthorizeExpression(method);
      List<KeyLocator> locator = findAuthorizeKeys(method);

      if (expression != null) {
        if (!locator.isEmpty()) {
          UUID principal = userRepository.findPrincipalId();
          return authorizeByRequest(delegate, ctx, req, principal, locator, expression);
        } else {
          LOGGER.warn("No authorization resource(s) found.");
          // going to assume the expression is just #deny, #permit or #defer
        }
      } else {
        LOGGER.debug("No authorization expression found.");
      }
    } else {
      LOGGER.warn("Couldn't unwrap service.");
    }

    return delegate.serve(ctx, req);
  }

  private HttpResponse authorizeByRequest(HttpService delegate, ServiceRequestContext ctx,
                                          HttpRequest req, UUID principal, List<KeyLocator> locators,
                                          String expression) throws Exception {
    //
    // Based on the query and payload parameters defined on the service method (that
    // have been gathered as Locators), we'll attempt to find the entity/resource that
    // we want to authorize against.

    Map<SecurableType, Object> resourceKeys = new HashMap<>();

    // Split up the locators by type, because we have to extract the value from the request
    // different ways for different types

    List<KeyLocator> systemLocators = locators.stream().filter(l -> l.getSource().equals(SYSTEM)).toList();
    List<KeyLocator> paramLocators = locators.stream().filter(l -> l.getSource().equals(PARAM)).toList();
    List<KeyLocator> payloadLocators = locators.stream().filter(l -> l.getSource().equals(PAYLOAD)).toList();

    // Add system-type keys, just metastore for now.
    systemLocators.forEach(l -> resourceKeys.put(l.getType(), "metastore"));

    // Extract the query/path parameter values just by grabbing them from the request
    paramLocators.forEach(l -> {
      String value = ctx.pathParam(l.getKey()) != null ? ctx.pathParam(l.getKey()) : ctx.queryParam(l.getKey());
      resourceKeys.put(l.getType(), value);
    });

    if (payloadLocators.isEmpty()) {
      // If we don't have any PAYLOAD locators, we're ready to evaluate the authorization and allow or deny
      // the request.
      LOGGER.debug("Checking authorization before method.");
      checkAuthorization(principal, expression, resourceKeys);

      return delegate.serve(ctx, req);
    } else {
      // Since we have PAYLOAD locators, we can only interrogate the payload while the request
      // is being evaluated, via peekData()
      LOGGER.debug("Checking authorization before in peekData.");

      PeekDataHandler peekDataHandler = new PeekDataHandler(req.contentType(), payloadLocators, resourceKeys);

      // Note that peekData only gets called for requests that actually have data (like PUT and POST)

      var peekReq = req.peekData(data -> {
        LOGGER.debug("Authorization peekData invoked.");

        if (peekDataHandler.processPeekData(data)) {
          checkAuthorization(principal, expression, resourceKeys);
        }
      });

      return delegate.serve(ctx, peekReq);
    }
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

  private void checkAuthorization(UUID principal, String expression, Map<SecurableType, Object> resourceKeys) {
    LOGGER.debug("resourceKeys = {}", resourceKeys);

    Map<SecurableType, Object> resourceIds = keyMapper.mapResourceKeys(resourceKeys);

    if (!resourceIds.keySet().containsAll(resourceKeys.keySet())) {
      LOGGER.warn("Some resource keys have unresolved ids.");
    }

    LOGGER.debug("resourceIds = {}", resourceIds);

    if (!evaluator.evaluate(principal, expression, resourceIds)) {
      throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
    }
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

  private static List<KeyLocator> findAuthorizeKeys(Method method) {
    // TODO: Cache this by method

    List<KeyLocator> locators = new ArrayList<>();

    AuthorizeKey methodKey = method.getAnnotation(AuthorizeKey.class);

    // If resource is on the method, its source is from a global/system variable
    if (methodKey != null) {
      locators.add(KeyLocator.builder().source(SYSTEM).type(methodKey.value()).build());
    }

    for (Parameter parameter : method.getParameters()) {
      AuthorizeKey paramKey = parameter.getAnnotation(AuthorizeKey.class);
      AuthorizeKeys paramKeys = parameter.getAnnotation(AuthorizeKeys.class);

      if (paramKey != null && paramKeys != null) {
        LOGGER.warn("Both AuthorizeKey and AuthorizeKeys present");
      }

      List<AuthorizeKey> allKeys = new ArrayList<>();
      if (paramKey != null) {
        allKeys.add(paramKey);
      }
      if (paramKeys != null) {
        allKeys.addAll(Arrays.asList(paramKeys.value()));
      }

      for (AuthorizeKey key : allKeys) {
        if (!key.key().isEmpty()) {
          // Explicitly declaring a key, so it's the source is from the payload data
          locators.add(KeyLocator.builder().source(PAYLOAD).type(key.value()).key(key.key()).build());
        } else {
          // No key defined so implicitly referencing an (annotated) (query) parameter
          Param param = parameter.getAnnotation(Param.class);
          if (param != null) {
            locators.add(KeyLocator.builder().source(PARAM).type(key.value()).key(param.value()).build());
          } else {
            LOGGER.warn("Couldn't find param key for authorization key");
          }
        }
      }
    }
    return locators;
  }

  private static Method findServiceMethod(HttpService httpService) throws ClassNotFoundException {
    if (httpService.unwrap() instanceof SimpleDecoratingHttpService decoratingService &&
            decoratingService.unwrap() instanceof AnnotatedService service) {

      LOGGER.debug("serviceName = {}, methodName = {}", service.serviceName(), service.methodName());

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
    private final List<KeyLocator> payloadLocators;
    private final Map<SecurableType, Object> resourceKeys;
    private final ByteArrayOutputStream dataStream = new ByteArrayOutputStream();

    private PeekDataHandler(MediaType contentType, List<KeyLocator> payloadLocators, Map<SecurableType, Object> resourceKeys) {
      this.contentType = contentType;
      this.payloadLocators = payloadLocators;
      this.resourceKeys = resourceKeys;
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
            Map<String, Object> payload = MAPPER.readValue(dataStream.toByteArray(), new TypeReference<>() {
            });

            payloadLocators.forEach(l -> resourceKeys.put(l.getType(), findPayloadValue(l.getKey(), payload)));
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