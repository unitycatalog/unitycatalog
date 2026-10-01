package io.unitycatalog.server.auth.annotation;

import io.unitycatalog.server.auth.decorator.AuthorizeValueExtractor;
import io.unitycatalog.server.model.SecurableType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Map a request parameter to a unity catalog resource key.
 *
 * <p>Unlike {@link AuthorizeKey} which only exposes the raw value of ANY request field, this class
 * only annotates request fields that reference to resources and maps them to resource identifiers
 * (UUIDs). The resource key is used to retrieve the resource identifier, which is then used to
 * authorize the request. As an example, suppose you are making a request the retrieve a schema, the
 * parameter that contains the schema name might be defined in the request as:
 *
 * <p>@AuthorizeResourceKey(SCHEMA) @Param("full_Name") String fullName
 *
 * <p>This annotation would take the value of the fullName parameter and use it to retrieve the
 * schema resource identifier looking up the identifier from the persistence layer (database).
 *
 * <p>This annotation can be used multiple times per service method. The interpretation of the
 * annotation changes depending on how it is used.
 *
 * <p>Method level - When used at the method level, it maps a server-level attribute for the
 * request. Currently, the only server level attribute is the METASTORE. When used at the method
 * level, it is expected that the key is left unset.
 *
 * <p>Example:
 *
 * <p>@AuthorizeResourceKey(METASTORE) public void serviceMethod(...) { }
 *
 * <p>Method parameter level with Armeria @Param annotation - When used on a method parameter, and
 * the parameter also has annotated with the Armeria @Param annotation, the key is taken from
 * the @Param annotations value and that is what is used to retrieve the resource value. Since the
 * key is taken from the @Param, the key value in this annotation should be left unset; if it is set
 * explicitly, it must equal the @Param value or registration fails.
 *
 * <p>Example: Map the request "catalog" parameter to the CATALOG resource type.
 *
 * <p>public void serviceMethod(@Param("catalog") @AuthorizeResourceKey(CATALOG) String catalog) { }
 *
 * <p>Method parameter level on payload parameter - When used on a method parameter, and there is no
 * corresponding Armeria @Param annotation, the annotation key field is required. That key is used
 * to retrieve the resource value from corresponding field the request payload.
 *
 * <p>Example: Map the "catalog" field in the request payload to the CATALOG resource type.
 *
 * <pre>{@code
 * public void serviceMethod(
 *   @AuthorizeResourceKey(value = CATALOG, key = "catalog") CreateSchemaRequest request) { }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
@Repeatable(AuthorizeResourceKeys.class)
public @interface AuthorizeResourceKey {

  SecurableType value();

  String key() default "";

  /**
   * A custom extractor that computes the resource key from the bound request body, used when a
   * field lookup by {@link #key} cannot express it (e.g. the value depends on the contents of a
   * list). When set (non-default), {@link #key} is unused (the variable name comes from {@link
   * #value}) and no {@code @Param} may be present. Defaults to the sentinel {@link
   * AuthorizeValueExtractor} itself, meaning "no extractor: use {@link #key}".
   */
  Class<? extends AuthorizeValueExtractor> extractor() default AuthorizeValueExtractor.class;

  /**
   * Names a non-resource SpEL variable (an {@code @AuthorizeKey}); when it evaluates truthy this
   * resource key is skipped: not resolved to an id, and absent from the expression context. Used
   * for a dual-purpose endpoint where a resource applies to only one request shape (e.g. the
   * Iceberg {@code updateTable} {@code TABLE} key is skipped for a staged-create commit, where no
   * table row exists yet). Because the deciding variable may come from the body, the skip is
   * applied after request values are gathered and before ids are resolved. Empty (the default)
   * means always resolve.
   */
  String skipWhen() default "";
}
