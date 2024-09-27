package io.unitycatalog.server.auth.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Map multiple AuthorizeKeys to a request payload parameter.
 *
 * <p>In some cases, it may be necessary to authorize a request based on multiple keys in the
 * request payload object. In order to do this, you would define multiple AuthorizeKey annotations
 * in the service method signature by using this annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
public @interface AuthorizeKeys {
  AuthorizeKey[] value();
}
