package io.unitycatalog.spark

/**
 * Spark 4.0 / 4.1 do not have the Spark 4.2 view API, so this shim adds no view support.
 */
trait UCSingleCatalogViewSupport { self: UCSingleCatalog => }
