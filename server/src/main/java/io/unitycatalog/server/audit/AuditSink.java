package io.unitycatalog.server.audit;

/**
 * Receives one {@link AuditRecord} per issued storage credential, permission change, and queued
 * storage cleanup. Configure an implementation with {@code server.audit.sink}; it needs a public
 * no-arg constructor and must be on the server classpath.
 *
 * <ul>
 *   <li>{@link #record} is called concurrently, from request threads and from the server's own
 *       threads such as the storage cleanup, so implementations must be thread-safe.
 *   <li>{@link #record} runs on the calling thread. A sink that sends records to a remote service
 *       should queue them and send asynchronously, and flush in {@link #close}. For a drop that
 *       queues storage cleanup, the calling thread is the one committing the drop, so a slow sink
 *       holds that drop's database connection until {@link #record} returns.
 *   <li>A failure thrown from {@link #record} or {@link #close} is logged and never fails the
 *       request or the shutdown.
 *   <li>Compatibility: {@link AuditRecord} may gain new getters and builder methods; existing ones
 *       are never removed, renamed or retyped. Its enum fields may gain values, so handle unknown
 *       values. New methods on this interface are only added as {@code default} methods.
 * </ul>
 */
public interface AuditSink {
  void record(AuditRecord record);

  /**
   * Called once when the server shuts down. Records that arrive after it are dropped, but a request
   * that outlived the shutdown drain can still call {@link #record} while this runs or just after
   * it; a sink that stops on close should drop such records.
   */
  default void close() {}
}
