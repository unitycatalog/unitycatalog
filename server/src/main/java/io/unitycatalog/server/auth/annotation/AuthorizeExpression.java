package io.unitycatalog.server.auth.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Define an authorization expression for a service method.
 *
 * <p>The default implementation is expected to be a Spring Expression Language (SpEL) syntax
 * expression.
 *
 * <p>The expression context includes the following methods
 *
 * <p>#authorize(principal, resource, privilege) #authorizeAny(principal, resource, privilege ...)
 * #authorizeAll(principal, resource, privilege ...)
 *
 * <p>And the context will include the following variables
 *
 * <p>#principal - the (UUID) of the principal making the request #deny - constant evaluating to
 * false #catalog - the ID of the catalog associated with the request (if applicable) #schema - the
 * ID of the schema associated with the request (if applicable) #table - the ID of the table
 * associated with the request (if applicable) #metastore - the ID of the metastore/server (if
 * applicable)
 *
 * <p>Example: #authorize(#principal, #schema, 'USE_SCHEMA')
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthorizeExpression {
  String value() default "#deny";
}
