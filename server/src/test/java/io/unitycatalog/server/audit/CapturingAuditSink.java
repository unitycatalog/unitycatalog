package io.unitycatalog.server.audit;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** Test sink. Static so a server can load it by class name; call {@link #reset} per test. */
public class CapturingAuditSink implements AuditSink {
  public static final List<AuditRecord> RECORDS = new CopyOnWriteArrayList<>();
  public static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

  public static void reset() {
    RECORDS.clear();
    CLOSE_COUNT.set(0);
  }

  @Override
  public void record(AuditRecord record) {
    RECORDS.add(record);
  }

  @Override
  public void close() {
    CLOSE_COUNT.incrementAndGet();
  }
}
