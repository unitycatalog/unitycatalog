package io.unitycatalog.server.exception;

import static io.unitycatalog.server.delta.model.DeltaErrorType.ALREADY_EXISTS_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.BAD_REQUEST_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.COMMIT_STATE_UNKNOWN_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.COMMIT_VERSION_CONFLICT_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.INTERNAL_SERVER_ERROR_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.INVALID_PARAMETER_VALUE_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NOT_AUTHORIZED_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NOT_FOUND_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NOT_IMPLEMENTED_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NO_SUCH_CATALOG_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NO_SUCH_SCHEMA_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.NO_SUCH_TABLE_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.PERMISSION_DENIED_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.RESOURCE_EXHAUSTED_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.SERVICE_UNAVAILABLE_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.UNSUPPORTED_TABLE_FORMAT_EXCEPTION;
import static io.unitycatalog.server.delta.model.DeltaErrorType.UPDATE_REQUIREMENT_CONFLICT_EXCEPTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.delta.model.DeltaErrorType;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.persist.model.Privileges;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Pins the response each API sends for every {@link ErrorCode}, so a change to a status or an error
 * type on any of them shows up here. The switch in {@link #expectedResponses} is exhaustive, so a
 * new error code does not compile until its responses are written down.
 */
public class ErrorCodeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The status and error type that the UC REST, Iceberg REST and Delta APIs report for a code. */
  private record Responses(
      int ucStatus,
      int icebergStatus,
      Class<? extends Exception> icebergType,
      int deltaStatus,
      DeltaErrorType deltaType) {}

  private static Responses expectedResponses(ErrorCode errorCode) {
    return switch (errorCode) {
      case INVALID_ARGUMENT, FAILED_PRECONDITION ->
          new Responses(
              400, 400, BadRequestException.class, 400, INVALID_PARAMETER_VALUE_EXCEPTION);
      case UNSUPPORTED_TABLE_FORMAT ->
          new Responses(
              400, 400, BadRequestException.class, 400, UNSUPPORTED_TABLE_FORMAT_EXCEPTION);
      case OUT_OF_RANGE ->
          new Responses(400, 400, BadRequestException.class, 400, BAD_REQUEST_EXCEPTION);
      case NOT_FOUND -> new Responses(404, 404, NotFoundException.class, 404, NOT_FOUND_EXCEPTION);
      case CATALOG_NOT_FOUND ->
          new Responses(404, 404, NoSuchNamespaceException.class, 404, NO_SUCH_CATALOG_EXCEPTION);
      case SCHEMA_NOT_FOUND ->
          new Responses(404, 404, NoSuchNamespaceException.class, 404, NO_SUCH_SCHEMA_EXCEPTION);
      case TABLE_NOT_FOUND ->
          new Responses(404, 404, NoSuchTableException.class, 404, NO_SUCH_TABLE_EXCEPTION);
      case ALREADY_EXISTS ->
          new Responses(409, 409, AlreadyExistsException.class, 409, ALREADY_EXISTS_EXCEPTION);
      // The legacy already-exists codes answer 400 on UC REST for backward compatibility. Iceberg
      // REST reports the two it can raise, schema and table, as a 409 conflict.
      case RESOURCE_ALREADY_EXISTS,
          CATALOG_ALREADY_EXISTS,
          STORAGE_CREDENTIAL_ALREADY_EXISTS,
          EXTERNAL_LOCATION_ALREADY_EXISTS ->
          new Responses(400, 400, AlreadyExistsException.class, 409, ALREADY_EXISTS_EXCEPTION);
      case SCHEMA_ALREADY_EXISTS, TABLE_ALREADY_EXISTS ->
          new Responses(400, 409, AlreadyExistsException.class, 409, ALREADY_EXISTS_EXCEPTION);
      case PERMISSION_DENIED ->
          new Responses(403, 403, ForbiddenException.class, 403, PERMISSION_DENIED_EXCEPTION);
      case UNAUTHENTICATED ->
          new Responses(401, 401, NotAuthorizedException.class, 401, NOT_AUTHORIZED_EXCEPTION);
      case RESOURCE_EXHAUSTED ->
          new Responses(429, 429, RESTException.class, 429, RESOURCE_EXHAUSTED_EXCEPTION);
      case ABORTED ->
          new Responses(409, 409, RESTException.class, 409, COMMIT_VERSION_CONFLICT_EXCEPTION);
      case COMMIT_VERSION_CONFLICT ->
          new Responses(
              409, 409, CommitFailedException.class, 409, COMMIT_VERSION_CONFLICT_EXCEPTION);
      case UPDATE_REQUIREMENT_CONFLICT ->
          new Responses(
              409, 409, CommitFailedException.class, 409, UPDATE_REQUIREMENT_CONFLICT_EXCEPTION);
      case UNIMPLEMENTED ->
          new Responses(
              501, 501, UnsupportedOperationException.class, 501, NOT_IMPLEMENTED_EXCEPTION);
      case INTERNAL, DATA_LOSS ->
          new Responses(
              500, 500, ServiceFailureException.class, 500, INTERNAL_SERVER_ERROR_EXCEPTION);
      case COMMIT_STATE_UNKNOWN ->
          new Responses(
              500, 500, ServiceFailureException.class, 500, COMMIT_STATE_UNKNOWN_EXCEPTION);
      case UNAVAILABLE ->
          new Responses(
              503, 503, ServiceUnavailableException.class, 503, SERVICE_UNAVAILABLE_EXCEPTION);
      // Iceberg REST answers 422 for a request it understood but cannot carry out. The Delta API
      // has no 422, so it reports such a request as a bad one.
      case UNPROCESSABLE_ENTITY ->
          new Responses(
              422, 422, UnprocessableEntityException.class, 400, INVALID_PARAMETER_VALUE_EXCEPTION);
    };
  }

  @ParameterizedTest
  @EnumSource(ErrorCode.class)
  public void everyApiReportsTheErrorCode(ErrorCode errorCode) throws Exception {
    Responses expected = expectedResponses(errorCode);
    BaseException exception = new BaseException(errorCode, "error message");

    AggregatedHttpResponse uc = errorResponse(GlobalExceptionHandler.INSTANCE, exception);
    assertThat(uc.status().code()).isEqualTo(expected.ucStatus());
    assertThat(MAPPER.readTree(uc.contentUtf8()).path("error_code").asText())
        .isEqualTo(errorCode.name());

    AggregatedHttpResponse iceberg = errorResponse(IcebergRestExceptionHandler.INSTANCE, exception);
    ErrorResponse icebergError = ErrorResponseParser.fromJson(iceberg.contentUtf8());
    assertThat(iceberg.status().code()).isEqualTo(expected.icebergStatus());
    assertThat(icebergError.code()).isEqualTo(expected.icebergStatus());
    assertThat(icebergError.type()).isEqualTo(expected.icebergType().getSimpleName());

    AggregatedHttpResponse delta = errorResponse(DeltaApiExceptionHandler.INSTANCE, exception);
    JsonNode deltaError = MAPPER.readTree(delta.contentUtf8()).path("error");
    assertThat(delta.status().code()).isEqualTo(expected.deltaStatus());
    assertThat(deltaError.path("code").asInt()).isEqualTo(expected.deltaStatus());
    assertThat(deltaError.path("type").asText()).isEqualTo(expected.deltaType().getValue());
  }

  @Test
  public void everyApiReportsAuthorizationUnavailable() throws Exception {
    UnityCatalogAuthorizer authorizer = mock(UnityCatalogAuthorizer.class);
    UUID principal = UUID.randomUUID();
    UUID table = UUID.randomUUID();
    BaseException unavailable =
        new BaseException(
            ErrorCode.UNAVAILABLE,
            "Authorization service is unavailable",
            new IOException("timeout"));
    when(authorizer.authorize(principal, SecurableType.TABLE, table, Privileges.OWNER))
        .thenThrow(unavailable);
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);
    Throwable failure =
        catchThrowable(
            () ->
                evaluator.evaluate(
                    principal,
                    "#authorize(#principal, #table, OWNER)",
                    Map.of(SecurableType.TABLE, table),
                    Map.of()));
    assertThat(failure).isSameAs(unavailable);

    for (BaseExceptionHandler handler :
        List.of(
            GlobalExceptionHandler.INSTANCE,
            IcebergRestExceptionHandler.INSTANCE,
            DeltaApiExceptionHandler.INSTANCE)) {
      AggregatedHttpResponse response = errorResponse(handler, failure);
      assertThat(response.status().code()).isEqualTo(503);
      assertThat(response.contentUtf8()).contains(unavailable.getErrorMessage());
    }
  }

  private static AggregatedHttpResponse errorResponse(
      BaseExceptionHandler handler, Throwable error) {
    HttpRequest request = HttpRequest.of(HttpMethod.GET, "/");
    return handler
        .handleException(ServiceRequestContext.of(request), request, error)
        .aggregate()
        .join();
  }
}
