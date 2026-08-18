package io.unitycatalog.server.auth.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Expose a request value as a variable in authorization expressions.
 *
 * <p>Unlike {@link AuthorizeResourceKey}, which annotates request values that reference resources
 * and maps them to resource identifiers (UUIDs), this class can annotate ANY request value (body
 * field or URL/path parameter) whether it is a resource or not. It does NOT do the resource ID
 * mapping but exposes the raw value directly to the SpEL expression context, even if the value
 * references a resource.
 *
 * <p>This generally should not be used to annotate any fields that refer to a resource, unless the
 * expression just want to check if the field is set or not (like "#field==null").
 *
 * <p>This is useful for conditional authorization based on any request parameters like operation
 * types, flags, or if any field is set or null.
 *
 * <p>The source is chosen by whether the annotated parameter also carries {@code @Param}: if
 * present, the value is read from the URL query or path parameter (leave {@link #key} empty to
 * reuse the {@code @Param} value, or set it explicitly to the same string); if absent, the value is
 * read from the request body field named by {@link #key}.
 *
 * <p>The value is exposed as a SpEL variable with the same name as the key, prefixed with '#'. For
 * example, {@code @AuthorizeKey(key = "operation")} makes the operation value available as {@code
 * #operation} in the expression.
 *
 * <p><b>Supported Types:</b>
 *
 * <ul>
 *   <li>Primitives (String, Integer, Boolean, etc.)
 *   <li>Enums - exposed as their string representation (via toString())
 *   <li>Nested fields - use dot notation (e.g., "config.mode"), body source only
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <pre>{@code
 * @Post("")
 * @AuthorizeExpression("""
 *   #operation == 'READ'
 *     ? #authorize(#principal, #table, SELECT)
 *     : #authorize(#principal, #table, MODIFY)
 *   """)
 * public HttpResponse generateCredential(
 *   @AuthorizeResourceKey(value = TABLE, key = "table_id")
 *   @AuthorizeKey(key = "operation")
 *   GenerateTemporaryTableCredential request) { ... }
 * }</pre>
 *
 * <p><b>Null Handling:</b> If the specified field doesn't exist or is null, the variable will be
 * set to null in the SpEL context. Expressions should handle this using null checks: {@code
 * #operation == null || #operation == 'READ'}
 *
 * @see AuthorizeResourceKey for mapping payload fields to resource identifiers
 * @see AuthorizeExpression for defining authorization expressions
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface AuthorizeKey {
  /**
   * The key path to extract from the request. When the annotated parameter also carries
   * {@code @Param}, leave this empty (the default) to reuse the {@code @Param} value as both the
   * URL parameter name and the SpEL variable name; if set, it must equal the {@code @Param} value.
   * When there is no {@code @Param}, this names a request body field; nested fields via dot
   * notation (e.g., "config.operation") are supported for the body source.
   */
  String key() default "";
}
