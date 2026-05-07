package io.unitycatalog.server.auth.annotation;

import io.unitycatalog.server.auth.decorator.ResultFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.service.AuthorizedService;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark service methods that require response filtering for authorization.
 *
 * <p>When a method is annotated with {@code @ResponseAuthorizeFilter}, it indicates that the method
 * returns a list of resources that must be filtered based on the user's permissions. The service
 * method MUST call {@code applyResponseFilter()} to filter the results before returning them to the
 * client.
 *
 * <p>This annotation works in conjunction with {@link AuthorizeExpression} to implement
 * authorization for LIST operations. The {@link UnityAccessDecorator} creates a {@link
 * ResultFilter} instance and stores it in the request context. The service method retrieves this
 * filter and applies it to the response list by calling {@code applyResponseFilter()}.
 *
 * <p><b>Security Enforcement:</b> If a method is annotated with {@code @ResponseAuthorizeFilter}
 * but does not call the filter on successful responses, the decorator will throw a security
 * exception to prevent unauthorized data leakage.
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * @Get("/tables")
 * @AuthorizeExpression("""#authorize(#principal, #catalog, USE_CATALOG) &&
 *                         #authorize(#principal, #schema, USE_SCHEMA)""")
 * @ResponseAuthorizeFilter
 * public HttpResponse listTables(
 *     @AuthorizeResourceKey(CATALOG) String catalogName,
 *     @AuthorizeResourceKey(SCHEMA) String schemaName) {
 *   List<TableInfo> tables = tableRepository.listTables(catalogName, schemaName);
 *   applyResponseFilter(TABLE, tables);  // REQUIRED - filters based on user permissions
 *   return HttpResponse.ofJson(new ListTablesResponse().tables(tables));
 * }
 * }</pre>
 *
 * @see AuthorizeExpression
 * @see ResultFilter
 * @see AuthorizedService#applyResponseFilter
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ResponseAuthorizeFilter {}
