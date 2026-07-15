package org.apache.spark.sql.connector.catalog

/**
 * Stub for the real `RelationCatalog` interface that Spark 4.2 introduced in
 * [SPARK-52729](https://github.com/apache/spark/pull/51419) (modeling tables and views as a
 * common `Relation`). On Spark 4.0 and 4.1 the real interface does not exist on the classpath,
 * so the connector's `with RelationCatalog` mixin would fail to verify at class-loading time.
 * This stub lets the connector compile against all three Spark versions while leaving the actual
 * view-side implementation to per-version `UCSingleCatalogViewSupport` shims.
 *
 * On Spark 4.2 the real Spark interface is on the classpath, so this stub file is absent under
 * `scala-shims/spark-4.2/`. The Delta connector uses the same fake-interface trick for
 * `SupportsV1OverwriteWithSaveAsTable` (see Delta's
 * `spark/src/main/scala-shims/spark-4.0/SupportsV1OverwriteWithSaveAsTable.scala`).
 */
trait RelationCatalog extends TableCatalog
