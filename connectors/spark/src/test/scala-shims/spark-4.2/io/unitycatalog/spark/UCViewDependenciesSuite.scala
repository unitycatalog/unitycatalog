package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class UCViewDependenciesSuite {

  private def deps(sql: String): Seq[String] =
    UCViewDependencies.collectTableDependencies(
      CatalystSqlParser.parsePlan(sql), "main", Seq("default"), caseSensitive = false)

  @Test
  def qualifiesUnqualifiedNameWithCatalogAndNamespace(): Unit =
    assertEquals(Seq("main.default.numbers"), deps("SELECT * FROM numbers"))

  @Test
  def qualifiesTwoPartNameWithCatalog(): Unit =
    assertEquals(Seq("main.sales.numbers"), deps("SELECT * FROM sales.numbers"))

  @Test
  def keepsThreePartNameAsIs(): Unit =
    assertEquals(Seq("other.sales.numbers"), deps("SELECT * FROM other.sales.numbers"))

  @Test
  def collectsBothJoinSources(): Unit =
    assertEquals(
      Set("main.default.a", "main.default.b"),
      deps("SELECT * FROM a JOIN b ON a.id = b.id").toSet)

  @Test
  def collectsRelationInsideWhereSubquery(): Unit =
    assertEquals(
      Set("main.default.a", "main.default.b"),
      deps("SELECT * FROM a WHERE id IN (SELECT id FROM b)").toSet)

  @Test
  def collectsBothUnionSources(): Unit =
    assertEquals(
      Set("main.default.a", "main.default.b"),
      deps("SELECT * FROM a UNION SELECT * FROM b").toSet)

  @Test
  def deduplicatesRepeatedReferences(): Unit =
    assertEquals(Seq("main.default.a"), deps("SELECT * FROM a UNION ALL SELECT * FROM a"))

  @Test
  def dropsReferencesWithMoreThanThreeParts(): Unit =
    assertTrue(deps("SELECT * FROM a.b.c.d").isEmpty)
}
