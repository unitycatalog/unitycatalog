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
 * <ul>
 *   <li>#authorize(principal, resource, privilege)
 *   <li>#authorizeAny(principal, resource, privilege ...)
 *   <li>#authorizeAll(principal, resource, privilege ...)
 * </ul>
 *
 * <p>And the context will include the following variables
 *
 * <ul>
 *   <li>#principal - the (UUID) of the principal making the request
 *   <li>#deny - constant evaluating to false
 *   <li>#defer - constant evaluating to true - a marker to note authorization happens elsewhere
 *   <li>#permit - constant evaluating to true - always allow access
 *   <li>#catalog - the ID of the catalog associated with the request (if applicable)
 *   <li>#schema - the ID of the schema associated with the request (if applicable)
 *   <li>#table - the ID of the table associated with the request (if applicable)
 *   <li>#metastore - the ID of the metastore/server (if applicable)
 * </ul>
 *
 * <p>Example: #authorize(#principal, #schema, 'USE_SCHEMA')
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthorizeExpression {
  String value() default "#deny";
}
