/**
 * The Java migration steps: steps that change rows, one {@link
 * io.unitycatalog.server.migrate.DataStep} per class, named {@code V<version>__<Description>}.
 * Versions are shared with the SQL steps of {@code db/migration/<database>}, and a merged step is
 * never edited. Steps use plain JDBC, nothing from {@code io.unitycatalog.server.persist}: entities
 * and repositories change with each release.
 */
package io.unitycatalog.server.migrate.steps;
