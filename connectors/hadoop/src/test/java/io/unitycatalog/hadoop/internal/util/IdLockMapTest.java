package io.unitycatalog.hadoop.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IdLockMapTest {

  @Test
  void acquireRejectsNullKey() {
    IdLockMap<String> locks = new IdLockMap<>();

    assertThatThrownBy(() -> locks.acquire(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("key cannot be null");
  }

  @Test
  void closeRemovesUnusedLock() {
    IdLockMap<String> locks = new IdLockMap<>();

    try (IdLockMap<String>.IdLock ignored = locks.acquire("k")) {
      assertThat(locks.lockCount()).isEqualTo(1);
      assertThat(locks.referenceCount("k")).isEqualTo(1);
    }

    assertThat(locks.lockCount()).isZero();
    assertThat(locks.referenceCount("k")).isZero();
  }

  @Test
  void sameKeyWaiterDoesNotEnterUntilCurrentHolderCloses() throws Exception {
    IdLockMap<String> locks = new IdLockMap<>();
    CountDownLatch waiterStarted = new CountDownLatch(1);
    CountDownLatch waiterEntered = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    try (IdLockMap<String>.IdLock ignored = locks.acquire("k")) {
      Future<?> waiter =
          executor.submit(
              () -> {
                waiterStarted.countDown();
                try (IdLockMap<String>.IdLock ignoredWaiter = locks.acquire("k")) {
                  waiterEntered.countDown();
                }
                return null;
              });

      assertThat(waiterStarted.await(5, TimeUnit.SECONDS)).isTrue();
      waitUntil(() -> locks.referenceCount("k") == 2);
      assertThat(waiterEntered.await(100, TimeUnit.MILLISECONDS)).isFalse();

      ignored.close();
      waiter.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertThat(locks.lockCount()).isZero();
  }

  @Test
  void differentKeysProgressIndependently() throws Exception {
    IdLockMap<String> locks = new IdLockMap<>();
    CountDownLatch otherKeyEntered = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    try (IdLockMap<String>.IdLock ignored = locks.acquire("a")) {
      Future<?> otherKey =
          executor.submit(
              () -> {
                try (IdLockMap<String>.IdLock ignoredOther = locks.acquire("b")) {
                  otherKeyEntered.countDown();
                }
                return null;
              });

      assertThat(otherKeyEntered.await(5, TimeUnit.SECONDS)).isTrue();
      otherKey.get(5, TimeUnit.SECONDS);
      assertThat(locks.referenceCount("a")).isEqualTo(1);
      assertThat(locks.referenceCount("b")).isZero();
    } finally {
      executor.shutdownNow();
    }

    assertThat(locks.lockCount()).isZero();
  }

  @Test
  void manyThreadsContendOnSameKeySerially() throws Exception {
    IdLockMap<String> locks = new IdLockMap<>();
    int threads = 64;
    CyclicBarrier startBarrier = new CyclicBarrier(threads);
    AtomicInteger enteredHolders = new AtomicInteger();
    AtomicInteger activeHolders = new AtomicInteger();
    AtomicInteger maxActiveHolders = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();

    try {
      try (IdLockMap<String>.IdLock ignored = locks.acquire("k")) {
        for (int i = 0; i < threads; i++) {
          futures.add(
              executor.submit(
                  () -> {
                    startBarrier.await(5, TimeUnit.SECONDS);
                    try (IdLockMap<String>.IdLock ignoredWaiter = locks.acquire("k")) {
                      int active = activeHolders.incrementAndGet();
                      try {
                        enteredHolders.incrementAndGet();
                        maxActiveHolders.accumulateAndGet(active, Math::max);
                        assertThat(active).isEqualTo(1);
                        Thread.yield();
                      } finally {
                        activeHolders.decrementAndGet();
                      }
                    }
                    return null;
                  }));
        }

        waitUntil(() -> locks.referenceCount("k") == threads + 1);
        assertThat(enteredHolders).hasValue(0);
      }

      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(enteredHolders).hasValue(threads);
    assertThat(maxActiveHolders).hasValue(1);
    assertThat(locks.lockCount()).isZero();
    assertThat(locks.referenceCount("k")).isZero();
  }

  private static void waitUntil(BooleanSupplier assertion) throws Exception {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!assertion.getAsBoolean()) {
      if (System.nanoTime() >= deadlineNanos) {
        fail("condition was not met in time");
      }
      Thread.sleep(10);
    }
  }

  @FunctionalInterface
  private interface BooleanSupplier {
    boolean getAsBoolean();
  }
}
