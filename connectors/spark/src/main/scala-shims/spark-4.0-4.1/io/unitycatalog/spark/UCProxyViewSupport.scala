package io.unitycatalog.spark

/**
 * Spark 4.0 / 4.1 do not have the Spark 4.2 view API (`RelationCatalog`, `View`,
 * `loadRelation`, `createView`, `loadView`, ...), so this shim adds no view support to `UCProxy`.
 * The shared `UCProxy.loadTable` already surfaces any view-like UC `tableType`
 * ([[UCViewTypes.isViewLikeTableType]]) as `NoSuchTableException` without referencing any
 * view-only type -- which cannot occur on a Spark version without view support anyway -- so this
 * trait is intentionally empty.
 */
trait UCProxyViewSupport { self: UCProxy => }
