package io.unitycatalog.spark.compat

import java.net.URI
import java.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test

class SparkCatalogCompatibilitySuite {

  @Test
  def catalogStorageFormatWithLocationPreservesLocationAndProperties(): Unit = {
    val location = URI.create("file:///tmp/uc-managed-table")
    val properties = new util.HashMap[String, String]()
    properties.put("delta.appendOnly", "true")

    val storage = SparkCatalogCompatibility.catalogStorageFormatWithLocation(location, properties)

    assertEquals(Some(location), storage.locationUri)
    assertEquals("true", storage.properties("delta.appendOnly"))

    val copyMethod = storage.getClass.getMethods.find(_.getName == "copy").orNull
    assertNotNull(copyMethod)
    assertTrue(copyMethod.getParameterCount == 6 || copyMethod.getParameterCount == 7)

    storage.getClass.getMethods.find(_.getName == "serdeName").foreach { serdeNameMethod =>
      assertEquals(None, serdeNameMethod.invoke(storage))
    }
  }
}
