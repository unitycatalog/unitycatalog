package io.unitycatalog.server.exception;

/**
 * Base for an exception a {@code TransactionManager} operation throws purely to roll its
 * transaction back, not to report an error. {@code TransactionManager} rolls back and rethrows such
 * an exception as-is (rather than wrapping it into an {@code INTERNAL} error), so the caller can
 * catch its specific type and translate it into a normal outcome.
 *
 * <p>Unchecked so it propagates through the (undeclared) operation call chain, and an exception
 * base class rather than a marker interface so {@code TransactionManager} can rethrow it
 * type-safely without an unchecked cast. Subclasses must be caught by their originating caller;
 * they are never meant to reach the HTTP layer.
 */
public abstract class TransactionRollbackException extends RuntimeException {}
