package io.unitycatalog.server.auth;

import io.unitycatalog.server.auth.annotation.AuthorizeExpression;

/**
 * Shared {@link AuthorizeExpression} string constants.
 *
 * <p>When the same logical operation is exposed through multiple endpoints (e.g. the UC REST API
 * and the UC Delta API both vending table credentials), each endpoint's
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
   * Authorization policy for reading table metadata (UC REST {@code GET /tables/{name}}, UC Delta
   * API, and the Iceberg REST {@code loadTable}). Metastore admin and catalog owner pass
   * unconditionally; schema owner passes with catalog {@code USE_CATALOG}; regular callers need
   * {@code USE_SCHEMA} + {@code USE_CATALOG} plus any of {@code OWNER} / {@code SELECT} /
   * {@code MODIFY} on the table itself.
   */
  public static final String GET_TABLE =
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorizeAny(#principal, #table, OWNER, SELECT, MODIFY))
      """;

  /**
   * Catalog-level read access: metastore admin, or {@code OWNER}/{@code USE_CATALOG} on the
   * catalog. Shared by the catalog get/list endpoints and the Delta/Iceberg config endpoints.
   */
  public static final String GET_CATALOG =
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
      """;

  /**
   * Schema-level read access: metastore admin, catalog {@code OWNER}, or schema
   * {@code OWNER}/{@code USE_SCHEMA} with catalog {@code USE_CATALOG}. Used as the get-schema gate
   * and the list-schemas/namespaces response filter (listing and reading grant the same access).
   */
  public static final String GET_SCHEMA =
      """
      #authorize(#principal, #metastore, OWNER) ||
      #authorize(#principal, #catalog, OWNER) ||
      (#authorizeAny(#principal, #catalog, USE_CATALOG) &&
          #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA))
      """;

  /**
   * Catalog tier for creating a schema: catalog {@code OWNER}, or {@code USE_CATALOG} +
   * {@code CREATE_SCHEMA}. Used directly by Iceberg {@code createNamespace} (no storage root);
   * {@link #CREATE_SCHEMA_WITH_STORAGE_ROOT} wraps it for the UC REST endpoint.
   */
  public static final String CREATE_SCHEMA =
      """
      #authorize(#principal, #catalog, OWNER) ||
      #authorizeAll(#principal, #catalog, USE_CATALOG, CREATE_SCHEMA)
      """;

  /**
   * UC REST create-schema policy: the {@link #CREATE_SCHEMA} catalog tier plus, when a
   * {@code storage_root} is supplied, {@code OWNER}/{@code CREATE_MANAGED_STORAGE} on the covering
   * (non-overlapping) external location. Mirrors the createCatalog storage-root gate.
   */
  public static final String CREATE_SCHEMA_WITH_STORAGE_ROOT =
      "("
          + CREATE_SCHEMA
          + """
          ) &&
          (#storage_root == null ||
           (#no_overlap_with_data_securable &&
            #external_location != null &&
            #authorizeAny(#principal, #external_location, OWNER, CREATE_MANAGED_STORAGE)))
          """;

  /**
   * Authorization policy for creating a staging table (UC REST {@code POST /staging-tables} and
   * UC Delta API {@code createStagingTable}). Catalog {@code USE_CATALOG}/{@code OWNER}
   * plus either schema {@code OWNER} or schema {@code USE_SCHEMA}+{@code CREATE_TABLE}. Catalog
   * OWNER alone is not sufficient.
   */
  public static final String CREATE_STAGING_TABLE =
      """
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
        && #authorize(#principal, #schema, OWNER)) ||
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG)
        && #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE))
      """;

  /**
   * Authorization policy for creating a table (UC REST {@code POST /tables} and UC Delta API
   * {@code createTable}). Catalog {@code USE_CATALOG}/{@code OWNER} plus either schema
   * {@code OWNER} or schema {@code USE_SCHEMA}+{@code CREATE_TABLE}. For EXTERNAL tables, the
   * caller additionally needs {@code OWNER}/{@code CREATE_EXTERNAL_TABLE} on the external location
   * (if one resolves) and the storage path must not overlap a data securable.
   *
   * <p>The {@code #table_type} SpEL variable comes from {@code @AuthorizeKey(key = "table-type")};
   * kebab-case payload keys surface with hyphens mapped to underscores (see {@link
   * io.unitycatalog.server.auth.decorator.AuthorizeKeyLocator#getVariableName}).
   */
  public static final String CREATE_TABLE =
      """
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#authorize(#principal, #schema, OWNER) ||
        #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE)) &&
      (#table_type != 'EXTERNAL' ||
        (#no_overlap_with_data_securable &&
          (#external_location == null ||
           #authorizeAny(#principal, #external_location, OWNER, CREATE_EXTERNAL_TABLE))))
      """;

  /**
   * Iceberg {@code createTable} policy: same as {@link #CREATE_TABLE}, but discriminates MANAGED
   * vs EXTERNAL on the request's {@code location} (Iceberg has no table_type). Keying on location
   * (not the resolved external location) keeps the no-overlap guard running when a path resolves
   * to no external location.
   */
  public static final String CREATE_ICEBERG_TABLE =
      """
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      (#authorize(#principal, #schema, OWNER) ||
        #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE)) &&
      (#location == null ||
        (#no_overlap_with_data_securable &&
          (#external_location == null ||
           #authorizeAny(#principal, #external_location, OWNER, CREATE_EXTERNAL_TABLE))))
      """;

  /**
   * Authorization policy for updating / committing to a table (UC REST {@code
   * DeltaCommitsService.postCommit} and the Delta {@code updateTable}). The Delta {@code POST
   * /tables/{name}} endpoint covers both metadata-only updates (properties, columns, comment,
   * protocol, domain metadata) and CCv2 commits, so the privilege bundle is the same as the UC REST
   * commit path: USE_CATALOG on catalog, USE_SCHEMA on schema, and both SELECT and MODIFY on the
   * table (OWNER satisfies each tier). SELECT is required alongside MODIFY because a writer must
   * also be able to read the table it commits to.
   */
  public static final String UPDATE_TABLE =
      """
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      (#authorize(#principal, #table, OWNER) ||
          #authorizeAll(#principal, #table, SELECT, MODIFY))
      """;

  /**
   * Authorization policy for deleting a table, shared by the UC REST, UC Delta API, and Iceberg
   * REST delete endpoints. Metastore admin alone is intentionally not sufficient -- the caller
   * must hold {@code OWNER} somewhere in the catalog / schema / table hierarchy.
   */
  public static final String DELETE_TABLE =
      """
      #authorize(#principal, #catalog, OWNER) ||
      (#authorize(#principal, #catalog, USE_CATALOG) && #authorize(#principal, #schema, OWNER)) ||
      (#authorize(#principal, #catalog, USE_CATALOG) &&
          #authorize(#principal, #schema, USE_SCHEMA) &&
          #authorize(#principal, #table, OWNER))
      """;

  /**
   * Authorization policy for renaming a table. Rename requires permission to delete the existing
   * table name as well as permission to create the new name in the same schema.
   */
  public static final String RENAME_TABLE =
      "(" + DELETE_TABLE + """
      ) &&
      (#authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
        (#authorize(#principal, #schema, OWNER) ||
          #authorizeAll(#principal, #schema, USE_SCHEMA, CREATE_TABLE)))
      """;

  /**
   * Authorization policy for vending table credentials. Admin-above-the-table privileges on
   * their own are not sufficient; the caller must have an explicit table-level privilege
   * matching the requested operation. {@code READ} needs OWNER or SELECT; {@code READ_WRITE}
   * needs OWNER, or both SELECT and MODIFY.
   */
  public static final String VEND_TABLE_CREDENTIAL =
      """
      #authorizeAny(#principal, #catalog, OWNER, USE_CATALOG) &&
      #authorizeAny(#principal, #schema, OWNER, USE_SCHEMA) &&
      (#operation == 'READ'
          ? #authorizeAny(#principal, #table, OWNER, SELECT)
          : (#authorize(#principal, #table, OWNER) ||
              #authorizeAll(#principal, #table, SELECT, MODIFY)))
      """;

  /**
   * Authorization policy for the {@code get*Authorization} (permission read) endpoints. These
   * endpoints check authorization themselves and tailor the response based on whether the principal
   * is an owner, so the only requirement here is that the caller is authenticated.
   */
  public static final String GET_RESOURCE_AUTHORIZATION = "#principal != null";
}
