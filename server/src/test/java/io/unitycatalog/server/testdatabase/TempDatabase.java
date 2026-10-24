package io.unitycatalog.server.testdatabase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/** Injects a fresh TestDatabase: per test for an instance field, per class for a static one. */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(TempDatabaseExtension.class)
public @interface TempDatabase {
  /** Inject a database without tables ({@link TestDatabases#newEmptyDatabase()}). */
  boolean empty() default false;
}
