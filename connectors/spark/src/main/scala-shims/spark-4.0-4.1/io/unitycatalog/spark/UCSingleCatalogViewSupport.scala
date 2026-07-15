package io.unitycatalog.spark

/**
 * Spark 4.0 / 4.1 do not have the Spark 4.2 view API (`RelationCatalog`, `View`,
 * `loadRelation`, `createView`, `loadView`, ...), so this shim adds no view support to
 * `UCSingleCatalog`. The `with RelationCatalog` mixin on `UCSingleCatalog` resolves to the
 * [[org.apache.spark.sql.connector.catalog.RelationCatalog]] stub trait (`extends TableCatalog`,
 * no abstract `loadRelation`) also defined in this shim directory, so the shared
 * [[UCSingleCatalog]] body compiles and runs without referencing any view-only type. This trait
 * is intentionally empty.
 */
trait UCSingleCatalogViewSupport { self: UCSingleCatalog => }
