package io.unitycatalog.server.auth.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Container for repeated {@link AuthorizeKey} annotations on a single parameter, so one payload
 * parameter can expose more than one SpEL variable (e.g. a table type and a staged-create flag).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface AuthorizeKeys {
  AuthorizeKey[] value();
}
