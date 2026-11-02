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
 *   <li>#permit - constant evaluating to true - always allow access
 *   <li>#catalog - the catalog resource associated with the request (if applicable)
 *   <li>#schema - the schema resource associated with the request (if applicable)
 *   <li>#table - the table resource associated with the request (if applicable)
 *   <li>#metastore - the metastore resource (if applicable)
 * </ul>
 *
 * <p>Resource variables contain a resource type and UUID. Pass them directly to the authorization
 * functions. Missing resources are null; compare them with null, not with UUIDs.
 *
 * <p>Example: #authorize(#principal, #schema, 'USE_SCHEMA')
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthorizeExpression {
  String value() default "#deny";
}
