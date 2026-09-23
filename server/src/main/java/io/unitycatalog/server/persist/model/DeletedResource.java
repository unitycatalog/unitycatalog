package io.unitycatalog.server.persist.model;

import io.unitycatalog.server.model.SecurableType;

/**
 * A securable removed by a repository delete. {@code parentId} is the direct Casbin g2 parent
 * (schema for tables/volumes/functions/models; catalog for schemas). Null for top-level securables
 * such as catalogs.
 */
public record DeletedResource(SecurableType type, String id, String parentId) {}
