/*
 * Copyright (2024) The Unity Catalog Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.unitycatalog.spark

import io.unitycatalog.client.model.TableType

/**
 * View-type mapping for the connector, kept out of `UCSingleCatalog` so that catalog stays focused
 * on normal-table management. Owns the single source of truth for which UC `TableType`s are
 * view-like and how they map to Spark's `TableSummary` view-type strings, plus the small predicates
 * derived from it that the view dispatch / load / create paths share.
 *
 * Uses only types available on all supported Spark versions (no Spark-4.2-only `View` /
 * `RelationCatalog`), so it lives in the shared source tree and compiles on Spark 4.0/4.1/4.2.
 */
private[spark] object UCViewTypes {

  /**
   * Single source of truth for which UC `TableType` values the connector treats as
   * view-like, and how each maps onto Spark's `TableSummary` view-type strings:
   *
   *   - `Some(s)` means: appears in `listViews`, `loadRelation` builds a `View`, and
   *     `createView` accepts `s` as the requested
   *     `TableSummary` table-type string.
   *   - `None` means: appears in `listViews` (and is filtered out of `listTables`) so the
   *     UC server-managed lifecycle is not mis-classified as a table, but Spark has no
   *     `TableSummary` constant for it yet, so loading or creating fails with a clear
   *     "no Spark view type" error.
   *
   * Adding a future view kind = one new entry here. The three predicates below derive
   * from this map, so listing / dispatch / createView reject all stay in sync.
   */
  private val viewLikeUcTypes: Map[TableType, Option[String]] = Map(
    TableType.VIEW -> Some("VIEW"),
    TableType.METRIC_VIEW -> Some("METRIC_VIEW" /* TableSummary.METRIC_VIEW_TABLE_TYPE on Spark 4.2 */),
    TableType.MATERIALIZED_VIEW -> None
  )

  /**
   * Surface routing: is this UC type view-like at all (a key in `viewLikeUcTypes`)? Includes
   * listed-but-unmapped kinds like `MATERIALIZED_VIEW` (`None`). Used by `listTables` / `listViews`
   * / `loadRelation` / `dropTable` to decide which surface a row belongs to. Contrast with
   * [[isViewCommandsSupportedTableType]], which additionally requires a Spark mapping.
   */
  def isViewLikeTableType(tableType: TableType): Boolean = viewLikeUcTypes.contains(tableType)

  /**
   * Used by `toView` to label the direct `View` relation with the right Spark
   * `TableSummary` table-type. Throws for view-like UC types we list but cannot yet load
   * (e.g. `MATERIALIZED_VIEW` until Spark adds a `TableSummary.MATERIALIZED_VIEW_TABLE_TYPE`).
   */
  def ucTableTypeToSparkViewType(tableType: TableType): String =
    viewLikeUcTypes.get(tableType).flatten.getOrElse(
      throw new UnsupportedOperationException(
        s"No Spark TableSummary view-type string for UC table type: $tableType"))

  /**
   * Reverse lookup: given a Spark `TableSummary` view-type string supplied by the caller
   * via `View.properties().get(PROP_TABLE_TYPE)`, return the corresponding UC
   * `TableType` to stamp on `CreateTable.tableType`. Computed once from `viewLikeUcTypes`.
   */
  private lazy val sparkViewTypeToUcType: Map[String, TableType] =
    viewLikeUcTypes.collect { case (ucType, Some(sparkType)) => sparkType -> ucType }

  def sparkViewTypeToUcTableType(sparkType: String): Option[TableType] =
    sparkViewTypeToUcType.get(sparkType)

  /**
   * CRUD gating: can the connector actually load / drop this view kind (a key with a `Some(_)`
   * Spark mapping)? Stricter than [[isViewLikeTableType]] -- the gap is exactly the listed-but-
   * unmapped kinds (`None`, e.g. `MATERIALIZED_VIEW`), which appear in `listViews` for surface
   * correctness but are rejected from view CRUD commands. Used by view-side commands (`loadView`,
   * `dropView`, ...); the caller has a UC `TableType` (e.g. from `getUCTableLike(...)`).
   * (The create path gates differently, via `sparkViewTypeToUcTableType` on the `PROP_TABLE_TYPE`
   * string.)
   */
  def isViewCommandsSupportedTableType(ucType: TableType): Boolean =
    viewLikeUcTypes.get(ucType).exists(_.isDefined)

  /**
   * Maps a *non-view* UC `TableType` onto the Spark `TableSummary` table-type string used by the
   * metadata-only `SHOW TABLES` listing path (`listTableSummaries`). Kept here as string literals
   * (not `TableSummary.*` constants) so this stays free of Spark-4.2-only types and compiles on
   * all supported Spark versions, mirroring the `"METRIC_VIEW"` literal in `viewLikeUcTypes`.
   *
   * The mapping stays consistent with what the credential-vending load path
   * (`getUCTableLike` -> `loadV1Table`) would produce, so a table's summary type matches the type
   * it would resolve to under the default `listTableSummaries` (which reads `PROP_TABLE_TYPE` off
   * the loaded table and falls back to `FOREIGN` when absent) -- except it derives the type from
   * the list-RPC row's `getTableType()` instead of a per-table `loadTable`, so no storage
   * credentials are vended:
   *
   *   - `MANAGED`          -> `"MANAGED"`   (`TableSummary.MANAGED_TABLE_TYPE`)
   *   - `STREAMING_TABLE`  -> `"MANAGED"`   -- `getUCTableLike` fetches with
   *                                            `readStreamingTableAsManaged = true`, so the load
   *                                            path sees UC `MANAGED` and `loadV1Table` builds a
   *                                            `CatalogTableType.MANAGED` V1 table (see #1017);
   *                                            classify it the same way here
   *   - `EXTERNAL`         -> `"EXTERNAL"`  (`TableSummary.EXTERNAL_TABLE_TYPE`)
   *   - anything else      -> `"FOREIGN"`  (`TableSummary.FOREIGN_TABLE_TYPE`), matching the
   *                                         default's absent-property fallback
   *
   * Callers must have already filtered out view-like rows (via [[isViewLikeTableType]]); this is
   * the tables-only surface, so view kinds never reach here.
   */
  def ucTableTypeToSparkTableSummaryType(tableType: TableType): String = tableType match {
    case TableType.MANAGED => "MANAGED"
    case TableType.STREAMING_TABLE => "MANAGED"
    case TableType.EXTERNAL => "EXTERNAL"
    case _ => "FOREIGN"
  }
}
