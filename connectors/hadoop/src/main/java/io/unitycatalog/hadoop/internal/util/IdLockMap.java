package io.unitycatalog.hadoop.internal.util;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

final class IdLockMap<K> {
  private final ConcurrentHashMap<K, RefCountedLock> locks = new ConcurrentHashMap<>();

  IdLock acquire(K key) {
    Objects.requireNonNull(key, "key cannot be null");
    RefCountedLock ref =
        locks.compute(
            key,
            (ignored, existing) -> {
              RefCountedLock lock = existing == null ? new RefCountedLock() : existing;
              lock.references.incrementAndGet();
              return lock;
            });
    ref.lock.lock();
    return new IdLock(key, ref);
  }

  int lockCount() {
    return locks.size();
  }

  int referenceCount(K key) {
    RefCountedLock ref = locks.get(key);
    return ref == null ? 0 : ref.references.get();
  }

  private void release(K key, RefCountedLock ref) {
    locks.computeIfPresent(
        key,
        (ignored, existing) -> {
          if (existing != ref) {
            return existing;
          }
          return existing.references.decrementAndGet() == 0 ? null : existing;
        });
  }

  final class IdLock implements AutoCloseable {
    private final K key;
    private final RefCountedLock ref;
    private boolean closed;

    private IdLock(K key, RefCountedLock ref) {
      this.key = key;
      this.ref = ref;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      ref.lock.unlock();
      release(key, ref);
    }
  }

  private static final class RefCountedLock {
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger references = new AtomicInteger();
  }
}
