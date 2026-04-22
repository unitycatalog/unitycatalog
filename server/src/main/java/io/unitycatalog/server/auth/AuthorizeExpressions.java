package io.unitycatalog.server.auth;

import io.unitycatalog.server.auth.annotation.AuthorizeExpression;

/**
 * Shared {@link AuthorizeExpression} string constants.
 *
 * <p>When the same logical operation is exposed through multiple endpoints (e.g. the UC REST API
 * and the Delta REST Catalog API both vending table credentials), each endpoint's
 * {@code @AuthorizeExpression} must grant identical access -- otherwise a caller's permissions
 * depend on which URL they happen to hit. Extracting the expression here makes the two sites
 * share a single source of truth, so drift becomes a compile-time impossibility instead of a
 * runtime surprise.
 *
 * <p>Convention: each constant is named {@code <ACTION>_<RESOURCE>} (e.g.
 * {@link #VEND_TABLE_CREDENTIAL}) to describe the authorized operation, not the endpoint. Add
 * new constants here whenever a second call site needs the same policy.
 */
public final class AuthorizeExpressions {

  private AuthorizeExpressions() {}

  /**
   * Authorization policy for reading table metadata (UC REST {@code GET /tables/{name}} and Delta
   * REST Catalog {@code loadTable}). Metastore admin and catalog owner pass unconditionally;
   * schema owner passes with catalog {@code USE_CATALOG}; regular callers need {@code USE_SCHEMA}
   * + {@code USE_CATALOG} plus any of {@code OWNER} / {@code SELECT} / {@code MODIFY} on the
   * table itself.
   */
  public static final String GET_TABLE =
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #schema, OWNER) && #authorize(#principal, #catalog, USE_CATALOG)) ||
      (#authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """;

  /**
   * Authorization policy for vending table credentials. Admin-above-the-table privileges on
   * their own are not sufficient; the caller must have an explicit table-level privilege
   * matching the requested operation. {@code READ} needs OWNER or SELECT; {@code READ_WRITE}
   * needs OWNER, or both SELECT and MODIFY.
   */
  public static final String VEND_TABLE_CREDENTIAL =
      """
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#operation == 'READ'
          ? #authorizeAny(#principal, #table, OWNER, SELECT)
          : (#authorize(#principal, #table, OWNER) ||
              #authorizeAll(#principal, #table, SELECT, MODIFY)))
      """;
}
