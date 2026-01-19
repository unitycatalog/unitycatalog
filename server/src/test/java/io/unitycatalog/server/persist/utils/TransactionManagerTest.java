package io.unitycatalog.server.persist.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionManagerTest {

  @Mock private SessionFactory sessionFactory;
  @Mock private Session session;
  @Mock private Transaction transaction;

  @BeforeEach
  public void setup() {
    when(sessionFactory.openSession()).thenReturn(session);
    when(session.beginTransaction()).thenReturn(transaction);
  }

  @Test
  public void testSuccessfulTransaction() {
    // Setup
    String expectedResult = "Success";

    // Execute
    String result =
        TransactionManager.executeWithTransaction(
            sessionFactory, session -> expectedResult, "Error executing operation", false);

    // Verify
    assertThat(result).isEqualTo(expectedResult);
    verify(transaction).commit();
    verify(transaction, never()).rollback();
    verify(session, never()).setDefaultReadOnly(true);
  }

  @Test
  public void testReadOnlyTransaction() {
    // Execute
    TransactionManager.executeWithTransaction(
        sessionFactory, session -> "Success", "Error executing operation", true);

    // Verify
    verify(session).setDefaultReadOnly(true);
    verify(transaction).commit();
  }

  @Test
  public void testTransactionWithBaseException() {
    // Setup
    BaseException expectedException =
        new BaseException(ErrorCode.INVALID_ARGUMENT, "Test exception");

    // Execute and verify
    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    sessionFactory,
                    session -> {
                      throw expectedException;
                    },
                    "Error executing operation",
                    false))
        .isEqualTo(expectedException);

    verify(transaction).rollback();
    verify(transaction, never()).commit();
  }

  @Test
  public void testTransactionWithRuntimeException() {
    // Setup
    RuntimeException testException = new RuntimeException("Test runtime exception");
    String errorMessage = "Error executing operation";

    // Execute and verify
    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    sessionFactory,
                    session -> {
                      throw testException;
                    },
                    errorMessage,
                    false))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(errorMessage)
        .hasMessageContaining(testException.getMessage());

    verify(transaction).rollback();
    verify(transaction, never()).commit();
  }

  @Test
  public void testSessionClosedAfterTransaction() {
    // Execute successful transaction
    TransactionManager.executeWithTransaction(
        sessionFactory, session -> "Success", "Error executing operation", false);

    // Verify session is closed
    verify(session).close();

    // Reset for error case
    reset(session);
    when(sessionFactory.openSession()).thenReturn(session);
    when(session.beginTransaction()).thenReturn(transaction);
    doThrow(new RuntimeException("Test exception")).when(transaction).commit();

    // Execute failing transaction
    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    sessionFactory, session -> "Success", "Error executing operation", false))
        .isInstanceOf(BaseException.class);

    // Verify session is closed even after error
    verify(session).close();
  }

  @Test
  public void testWriteInReadOnlyTransactionFails() {
    // Setup
    Object entity = new Object();
    String errorMessage = "Attempt to write in read-only session";

    // Mock a write operation failure
    RuntimeException hibernateException =
        new RuntimeException("HibernateException: cannot write in read-only session");
    doThrow(hibernateException).when(session).persist(any());
    // Execute and verify
    assertThatThrownBy(
            () ->
                TransactionManager.executeWithTransaction(
                    sessionFactory,
                    session -> {
                      session.persist(entity); // This should fail in read-only mode
                      return null;
                    },
                    errorMessage,
                    /* readOnly = */ true))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining(errorMessage)
        .hasMessageContaining(hibernateException.getMessage());

    // Verify the transaction was rolled back
    verify(transaction).rollback();
    verify(transaction, never()).commit();

    // Verify session was set to read-only
    verify(session).setDefaultReadOnly(true);
  }
}
