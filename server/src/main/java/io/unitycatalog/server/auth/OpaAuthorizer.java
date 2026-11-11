package io.unitycatalog.server.auth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.ResponseTimeoutMode;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.client.redirect.RedirectConfig;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.JsonUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delegates each privilege decision to OPA. Native grants and hierarchy remain available for
 * permission management and resource bookkeeping, but do not affect OPA decisions.
 */
public class OpaAuthorizer implements UnityCatalogAuthorizer, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpaAuthorizer.class);
  private static final int MAX_RESPONSE_LENGTH = 64 * 1024;

  private final JCasbinAuthorizer nativeAuthorizer;
  private final OpaInputBuilder inputBuilder;
  private final WebClient webClient;
  private final String decisionUrl;

  /**
   * @param nativeAuthorizer stores native grants and hierarchy; closed with this authorizer
   * @param sessionFactory supplies identity and resource metadata
   * @param decisionUrl full OPA data API URL returning a boolean {@code result.allow}
   * @param responseTimeout response timeout for each OPA call
   */
  public OpaAuthorizer(
      JCasbinAuthorizer nativeAuthorizer,
      SessionFactory sessionFactory,
      String decisionUrl,
      Duration responseTimeout) {
    this.nativeAuthorizer = nativeAuthorizer;
    this.inputBuilder = new OpaInputBuilder(sessionFactory);
    this.decisionUrl = decisionUrl;
    this.webClient =
        WebClient.builder()
            .followRedirects(RedirectConfig.disabled())
            .responseTimeoutMode(ResponseTimeoutMode.FROM_START)
            .responseTimeout(responseTimeout)
            .contextCustomizer(
                ctx -> {
                  ServiceRequestContext root = ctx.root();
                  if (root != null) {
                    // Successful UC requests never complete the cancellation future.
                    AtomicReference<ClientRequestContext> pending = new AtomicReference<>(ctx);
                    root.whenRequestCancelling()
                        .thenAccept(
                            cause -> {
                              ClientRequestContext call = pending.getAndSet(null);
                              if (call != null) {
                                call.cancel(cause);
                              }
                            });
                    ctx.log().whenComplete().thenAccept(unused -> pending.set(null));
                  }
                })
            .maxResponseLength(MAX_RESPONSE_LENGTH)
            .build();
  }

  @Override
  public boolean authorize(
      UUID principal, SecurableType resourceType, UUID resource, Privileges action) {
    JsonNode input;
    try {
      input = inputBuilder.build(principal, resourceType, resource, action);
    } catch (RuntimeException e) {
      ErrorCode code =
          e instanceof BaseException failure ? failure.getErrorCode() : ErrorCode.INTERNAL;
      if (code != ErrorCode.NOT_FOUND) {
        throw new BaseException(code, "Could not build OPA authorization input", e);
      }
      LOGGER.warn(
          "Authorization check denied: principal={}, privilege={}, resource={} {}: {}",
          principal,
          action,
          resourceType,
          resource,
          e.getMessage());
      return false;
    }
    try {
      String request = JsonUtils.getInstance().writeValueAsString(input);
      AggregatedHttpResponse response =
          webClient
              .prepare()
              .post(decisionUrl)
              .content(MediaType.JSON, request)
              .execute()
              .aggregate()
              .join();
      if (!response.status().isSuccess()) {
        throw new BaseException(
            ErrorCode.UNAVAILABLE, "OPA returned HTTP " + response.status().code());
      }
      JsonNode body =
          JsonUtils.getInstance()
              .reader()
              .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
              .readTree(response.contentUtf8());
      JsonNode allow = body.path("result").path("allow");
      if (!allow.isBoolean()) {
        throw new BaseException(
            ErrorCode.UNAVAILABLE, "OPA response must contain a boolean result.allow");
      }
      return allow.booleanValue();
    } catch (BaseException e) {
      throw e;
    } catch (Exception e) {
      throw new BaseException(
          ErrorCode.UNAVAILABLE, "Could not get an authorization decision from OPA", e);
    }
  }

  @Override
  public boolean authorizeAny(
      UUID principal, SecurableType resourceType, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .anyMatch(action -> authorize(principal, resourceType, resource, action));
  }

  @Override
  public boolean authorizeAll(
      UUID principal, SecurableType resourceType, UUID resource, Privileges... actions) {
    return Arrays.stream(actions)
        .allMatch(action -> authorize(principal, resourceType, resource, action));
  }

  @Override
  public boolean grantAuthorization(UUID principal, UUID resource, Privileges action) {
    return nativeAuthorizer.grantAuthorization(principal, resource, action);
  }

  @Override
  public boolean revokeAuthorization(UUID principal, UUID resource, Privileges action) {
    return nativeAuthorizer.revokeAuthorization(principal, resource, action);
  }

  @Override
  public boolean clearAuthorizationsForPrincipal(UUID principal) {
    return nativeAuthorizer.clearAuthorizationsForPrincipal(principal);
  }

  @Override
  public boolean clearAuthorizationsForResource(UUID resource) {
    return nativeAuthorizer.clearAuthorizationsForResource(resource);
  }

  @Override
  public boolean addHierarchyChild(UUID parent, UUID child) {
    return nativeAuthorizer.addHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChild(UUID parent, UUID child) {
    return nativeAuthorizer.removeHierarchyChild(parent, child);
  }

  @Override
  public boolean removeHierarchyChildren(UUID resource) {
    return nativeAuthorizer.removeHierarchyChildren(resource);
  }

  @Override
  public UUID getHierarchyParent(UUID resource) {
    return nativeAuthorizer.getHierarchyParent(resource);
  }

  @Override
  public List<Privileges> listAuthorizations(UUID principal, UUID resource) {
    return nativeAuthorizer.listAuthorizations(principal, resource);
  }

  @Override
  public Map<UUID, List<Privileges>> listAuthorizations(UUID resource) {
    return nativeAuthorizer.listAuthorizations(resource);
  }

  @Override
  public void close() {
    nativeAuthorizer.close();
  }
}
