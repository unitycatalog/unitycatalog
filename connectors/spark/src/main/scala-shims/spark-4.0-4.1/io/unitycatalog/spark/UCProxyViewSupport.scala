package io.unitycatalog.spark

/**
 * Spark 4.0 / 4.1 do not support the metric-view API, so this shim leaves views unsupported.
 */
trait UCProxyViewSupport { self: UCProxy => }
