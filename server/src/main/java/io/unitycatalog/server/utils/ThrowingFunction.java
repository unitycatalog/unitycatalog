package io.unitycatalog.server.utils;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {
  R apply(T t) throws E;

  static <T, R, E extends Exception> Function<T, R> handleException(
      ThrowingFunction<T, R, E> throwingFunction, Class<E> exceptionClazz) {
    return i -> {
      try {
        return throwingFunction.apply(i);
      } catch (Exception e) {
        throw new RuntimeException("Exception caught in stream processing", e);
      }
    };
  }
}
