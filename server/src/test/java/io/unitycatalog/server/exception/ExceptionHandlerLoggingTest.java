package io.unitycatalog.server.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestId;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.unboundid.scim2.common.exceptions.ResourceConflictException;
import com.unboundid.scim2.common.exceptions.ServerErrorException;
import io.unitycatalog.server.logging.RequestIdContextDataProvider;
import io.unitycatalog.server.utils.LogCapture;
import java.util.List;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Each dialect's handler logs a 5xx once, with its cause and request ID, and a 4xx not at all. */
public class ExceptionHandlerLoggingTest {

  private static final RequestId REQUEST_ID = RequestId.of(42);

  private static final HttpRequest REQUEST =
      HttpRequest.of(HttpMethod.POST, "/api/2.1/unity-catalog/catalogs");

  static Stream<Arguments> handlers() {
    return Stream.of(
        Arguments.of("UC", GlobalExceptionHandler.INSTANCE),
        Arguments.of("Iceberg", IcebergRestExceptionHandler.INSTANCE),
        Arguments.of("Delta", DeltaApiExceptionHandler.INSTANCE));
  }

  private static ServiceRequestContext context() {
    return ServiceRequestContext.builder(REQUEST).id(REQUEST_ID).build();
  }

  /**
   * Handles {@code cause}, checks it is answered with {@code status}, and returns what it logged.
   */
  private static List<LogEvent> handle(
      ExceptionHandlerFunction handler, Throwable cause, HttpStatus status) {
    try (LogCapture logs = LogCapture.of(BaseExceptionHandler.class)) {
      assertThat(handler.handleException(context(), REQUEST, cause).aggregate().join().status())
          .isEqualTo(status);
      return logs.eventsAtLeast(Level.WARN);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("handlers")
  public void logsAServerErrorOnceWithItsCauseAndRequestId(
      String dialect, ExceptionHandlerFunction handler) {
    for (Throwable cause :
        List.of(
            new BaseException(ErrorCode.INTERNAL, "boom", new IllegalStateException("db down")),
            new IllegalStateException("unexpected"))) {
      List<LogEvent> errors = handle(handler, cause, HttpStatus.INTERNAL_SERVER_ERROR);
      assertThat(errors).hasSize(1);
      LogEvent error = errors.get(0);
      assertThat(error.getLevel()).isEqualTo(Level.ERROR);
      assertThat(error.getThrown()).isSameAs(cause);
      assertThat(error.getMessage().getFormattedMessage())
          .contains("POST /api/2.1/unity-catalog/catalogs")
          .contains("500");
      assertThat(error.getContextData().<String>getValue(RequestIdContextDataProvider.LOG_KEY))
          .isEqualTo(REQUEST_ID.text());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("handlers")
  public void doesNotLogAClientError(String dialect, ExceptionHandlerFunction handler) {
    assertThat(
            handle(
                handler,
                new BaseException(ErrorCode.NOT_FOUND, "no such catalog"),
                HttpStatus.NOT_FOUND))
        .isEmpty();
    assertThat(handle(handler, new IllegalArgumentException("bad value"), HttpStatus.BAD_REQUEST))
        .isEmpty();
  }

  @Test
  public void logsAScimServerErrorButNotAScimClientError() {
    Scim2RuntimeException serverError =
        new Scim2RuntimeException(new ServerErrorException("store unavailable"));
    List<LogEvent> errors =
        handle(GlobalExceptionHandler.INSTANCE, serverError, HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0).getThrown()).isSameAs(serverError);

    Scim2RuntimeException conflict =
        new Scim2RuntimeException(new ResourceConflictException("user exists"));
    assertThat(handle(GlobalExceptionHandler.INSTANCE, conflict, HttpStatus.INTERNAL_SERVER_ERROR))
        .isEmpty();
  }

  @Test
  public void logsUnderItsOwnIdWhenAnotherRequestIsCurrent() {
    ServiceRequestContext other =
        ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/other"));
    try (SafeCloseable ignored = other.push()) {
      List<LogEvent> errors =
          handle(
              GlobalExceptionHandler.INSTANCE,
              new IllegalStateException("unexpected"),
              HttpStatus.INTERNAL_SERVER_ERROR);
      assertThat(errors).hasSize(1);
      assertThat(
              errors.get(0).getContextData().<String>getValue(RequestIdContextDataProvider.LOG_KEY))
          .isEqualTo(REQUEST_ID.text());
    }
  }

  @Test
  public void stillAnswersWhenLoggingFails() {
    Logger logger = (Logger) LogManager.getLogger(BaseExceptionHandler.class);
    AbstractAppender failing =
        new AbstractAppender("failing", null, null, /* ignoreExceptions= */ false, null) {
          @Override
          public void append(LogEvent event) {
            throw new IllegalStateException("disk full");
          }
        };
    failing.start();
    logger.addAppender(failing);
    try {
      handle(
          GlobalExceptionHandler.INSTANCE,
          new IllegalStateException("unexpected"),
          HttpStatus.INTERNAL_SERVER_ERROR);
    } finally {
      logger.removeAppender(failing);
      failing.stop();
    }
  }
}
