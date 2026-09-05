package io.unitycatalog.server.exception;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.ServerErrorHandler;
import com.linecorp.armeria.server.ServiceConfig;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import lombok.SneakyThrows;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * Renders the status-only responses Armeria produces when no route matched -- 404 for a path the
 * server does not serve, 405 for a method that path does not accept -- as an Iceberg {@link
 * ErrorResponse}, for the paths that belong to the Iceberg REST API.
 *
 * <p>{@link IcebergRestExceptionHandler} can only speak for a request that reached the Iceberg
 * service. A request to an endpoint the service does not implement never gets that far, and Armeria
 * answers it with a plain-text body ("Status: 405") that an Iceberg client cannot parse. Clients
 * that consult the {@code endpoints} list this server advertises never make such a request, but the
 * ones that predate that field, or that skip it, get an unreadable response where the REST spec
 * says an error document belongs.
 *
 * <p>Paths outside the Iceberg API are left to Armeria: this handler returns null for them, and for
 * service exceptions, which the per-service handlers already render.
 */
public final class UnroutedIcebergRequestHandler implements ServerErrorHandler {

  private final String icebergPathPrefix;

  /**
   * @param icebergPathPrefix the path prefix the Iceberg REST API is served under, e.g. {@code
   *     /api/2.1/unity-catalog/iceberg/}
   */
  public UnroutedIcebergRequestHandler(String icebergPathPrefix) {
    this.icebergPathPrefix = icebergPathPrefix;
  }

  @Nullable
  @Override
  public HttpResponse onServiceException(ServiceRequestContext ctx, Throwable cause) {
    // An exception thrown by a service is rendered by that service's own handler.
    return null;
  }

  @SneakyThrows
  @Nullable
  @Override
  public AggregatedHttpResponse renderStatus(
      @Nullable ServiceConfig config,
      RequestHeaders headers,
      HttpStatus status,
      @Nullable String description,
      @Nullable Throwable cause) {
    if (!headers.path().startsWith(icebergPathPrefix)) {
      return null;
    }
    String message =
        description != null && !description.isEmpty() ? description : status.reasonPhrase();
    return AggregatedHttpResponse.of(
        status,
        MediaType.JSON,
        IcebergObjectMapper.mapper()
            .writeValueAsString(
                ErrorResponse.builder()
                    .responseCode(status.code())
                    .withType(errorType(status))
                    .withMessage(message)
                    .build()));
  }

  /**
   * The Iceberg exception to name for a status raised before any service was reached. A path the
   * server does not serve is reported as not found; Iceberg has no exception of its own for the
   * remaining statuses and raises a plain {@code RESTException} for them.
   */
  private static String errorType(HttpStatus status) {
    return status.code() == HttpStatus.NOT_FOUND.code()
        ? NotFoundException.class.getSimpleName()
        : RESTException.class.getSimpleName();
  }
}
