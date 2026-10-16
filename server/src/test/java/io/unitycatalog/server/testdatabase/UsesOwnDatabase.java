package io.unitycatalog.server.testdatabase;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marks a test class that connects to a database of its own choosing instead of the one selected by
 * {@value SupportedDatabase#ENV}, so {@link SelectedDatabaseCheck} lets it through.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(UsesOwnDatabaseExtension.class)
public @interface UsesOwnDatabase {
  /** Why this class does not run on the selected database. Must not be blank. */
  String reason();
}
