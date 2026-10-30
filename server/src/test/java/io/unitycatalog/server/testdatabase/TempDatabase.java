package io.unitycatalog.server.testdatabase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Injects a fresh TestDatabase: per test for an instance field, per class for a static one. A
 * {@code @Nested} test gets only its own class's fields injected, not the enclosing class's.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(TempDatabaseExtension.class)
public @interface TempDatabase {}
