package io.unitycatalog.server.testdatabase;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * Creates the {@link TestDatabase} for each {@link TempDatabase} field and puts it in the JUnit
 * store. BeforeEachCallback runs before any {@code @BeforeEach}, and the store closes its {@link
 * AutoCloseable} values after every {@code @AfterEach}, so a test's server closes before its
 * database is dropped, and a failing setUp still drops the database.
 */
final class TempDatabaseExtension implements BeforeAllCallback, BeforeEachCallback {

  @Override
  public void beforeAll(ExtensionContext context) throws IllegalAccessException {
    inject(context, null, true);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IllegalAccessException {
    inject(context, context.getRequiredTestInstance(), false);
  }

  private static void inject(ExtensionContext context, Object instance, boolean isStatic)
      throws IllegalAccessException {
    for (Field field :
        AnnotationSupport.findAnnotatedFields(context.getRequiredTestClass(), TempDatabase.class)) {
      if (Modifier.isStatic(field.getModifiers()) == isStatic) {
        TestDatabase database =
            field.getAnnotation(TempDatabase.class).empty()
                ? TestDatabases.newEmptyDatabase()
                : TestDatabases.newDatabase();
        context
            .getStore(ExtensionContext.Namespace.create(TempDatabaseExtension.class, field))
            .put("database", database);
        field.setAccessible(true);
        field.set(instance, database);
      }
    }
  }
}
