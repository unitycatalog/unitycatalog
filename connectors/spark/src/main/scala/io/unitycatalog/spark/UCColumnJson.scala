package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.types.DataType
import org.json4s.{JBool, JObject, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

/**
 * Helpers that round-trip a Spark V2 [[Column]] through the canonical "Spark `StructField`
 * shape" JSON that Unity Catalog stores in `ColumnInfo.type_json`. Wire-format compatible
 * with what Databricks Runtime emits via `StructField.toJson`.
 *
 * Implemented with json4s + Spark's already-public V2 surface only:
 *
 *   - [[Column.name]] / [[Column.dataType]] / [[Column.nullable]] / [[Column.comment]] /
 *     [[Column.metadataInJSON]] -- public accessors on the V2 catalog `Column` interface.
 *   - [[org.apache.spark.sql.types.DataType.json]] / [[DataType.fromJson]] -- public on
 *     Spark `DataType`.
 *   - [[Column.create]] (5-arg overload) -- public static factory on the V2 `Column`.
 *
 * No `private[sql]` helpers, no Spark API additions. The shape is just
 * `{name, type, nullable, metadata}`.
 *
 * Comment handling mirrors Spark's StructField wire convention: comments live inside
 * `metadata` under the `"comment"` key on the wire, but on the in-memory `Column` they are
 * exposed via the dedicated [[Column.comment]] accessor. The helpers merge / strip the
 * `"comment"` key at the JSON boundary so callers never see it duplicated.
 *
 * Uses only public V2 APIs that exist on all supported Spark versions, so this lives in the
 * shared source tree (not a `scala-shims/` dir) even though its only caller today is the
 * Spark-4.2 `UCProxyViewSupport` view shim.
 */
private[spark] object UCColumnJson {

  /** Spark V2 -> wire: build the `StructField`-shape JSON for one [[Column]]. */
  def buildStructFieldJson(col: Column): String = {
    val typeNode: JValue = parse(col.dataType.json)
    // metadataInJSON may be null for fields without analyzer-attached metadata.
    val baseMetadata: JObject = Option(col.metadataInJSON) match {
      case Some(s) => parse(s).asInstanceOf[JObject]
      case None    => JObject(Nil)
    }
    val metadataNode: JObject = Option(col.comment) match {
      case Some(c) => baseMetadata ~ ("comment" -> c)
      case None    => baseMetadata
    }
    val fieldNode: JValue =
      ("name"     -> col.name) ~
        ("type"     -> typeNode) ~
        ("nullable" -> col.nullable) ~
        ("metadata" -> metadataNode)
    compact(render(fieldNode))
  }

  /**
   * Wire -> Spark V2: parse a `StructField`-shape JSON string back into a [[Column]].
   *
   * The `"comment"` key (if present in the wire metadata) is lifted out and passed to
   * [[Column.create]] as the dedicated `comment` arg, NOT left in `metadataInJSON` --
   * matches what Spark's own `CatalogV2Util.structFieldToV2Column` does on the v1 path.
   */
  def parseStructFieldJson(jsonStr: String): Column = {
    // `ColumnInfo.type_json` is nullable on the wire (e.g. a view-like row created by an older
    // writer or a non-Spark client that did not round-trip the field). Fail with a clear,
    // actionable error rather than letting `json4s.parse(null)` throw a raw NPE out of the
    // view-load path.
    if (jsonStr == null) {
      throw new IllegalArgumentException(
        "Column type_json is missing; cannot reconstruct the Spark column for this view")
    }
    val parsed = parse(jsonStr)
    val name = (parsed \ "name") match {
      case JString(s) => s
      case other      => throw new IllegalArgumentException(
        s"Expected string `name` in StructField JSON, got: $other")
    }
    val typeNode = parsed \ "type"
    val dataType = DataType.fromJson(compact(render(typeNode)))
    val nullable = (parsed \ "nullable") match {
      case JBool(b) => b
      case other    => throw new IllegalArgumentException(
        s"Expected boolean `nullable` in StructField JSON, got: $other")
    }
    val metadataObj = (parsed \ "metadata") match {
      case obj: JObject => obj
      case _            => JObject(Nil)
    }
    val (commentFields, otherFields) = metadataObj.obj.partition(_._1 == "comment")
    val comment = commentFields.headOption.map(_._2) match {
      case Some(JString(s)) => s
      case _                => null
    }
    val metadataInJSON = otherFields match {
      case Nil => null
      case xs  => compact(render(JObject(xs)))
    }
    Column.create(name, dataType, nullable, comment, metadataInJSON)
  }
}
