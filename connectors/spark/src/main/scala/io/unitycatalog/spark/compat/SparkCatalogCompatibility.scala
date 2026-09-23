package io.unitycatalog.spark.compat

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat

import scala.collection.JavaConverters._

object SparkCatalogCompatibility {

  /**
   * Builds a CatalogStorageFormat across Spark versions.
   *
   * Spark 4.2 adds a `serdeName` field to CatalogStorageFormat.copy, while Spark 4.0 and
   * Spark 4.1 do not have that parameter. Keep the ABI-specific reflection in this shim so
   * connector code can use one compatibility entry point.
   */
  def catalogStorageFormatWithLocation(
      locationUri: URI,
      properties: util.Map[String, String]): CatalogStorageFormat = {
    val base = CatalogStorageFormat.empty
    val copyMethod = base.getClass.getMethods.find { method =>
      method.getName == "copy" && (method.getParameterCount == 6 || method.getParameterCount == 7)
    }.getOrElse {
      throw new IllegalStateException("Unsupported CatalogStorageFormat.copy signature")
    }
    val copyArgs = Array[AnyRef](
      Some(locationUri),
      base.inputFormat,
      base.outputFormat,
      base.serde,
      Boolean.box(base.compressed),
      properties.asScala.toMap
    )
    val effectiveArgs = copyMethod.getParameterCount match {
      case 6 => copyArgs
      case 7 => copyArgs :+ None
      case _ => throw new IllegalStateException("Unsupported CatalogStorageFormat.copy arity")
    }
    copyMethod.invoke(base, effectiveArgs: _*).asInstanceOf[CatalogStorageFormat]
  }
}
