/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}

/**
 * ResolveSessionCatalog exits early for some v2 View commands,
 * thus they are pre-substituted here and then handled in ResolveViews
 */
case class RewriteViewCommands(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case DropView(ResolvedIdent(resolved), ifExists) =>
      DropUCView(resolved, ifExists)

    case CreateView(ResolvedIdent(resolved), userSpecifiedColumns, comment, _, properties,
      Some(queryText), query, allowExisting, replace, _) =>
      val q = CTESubstitution.apply(query)
      CreateUCView(child = resolved,
        queryText = queryText,
        query = q,
        columnAliases = userSpecifiedColumns.map(_._1),
        columnComments = userSpecifiedColumns.map(_._2.orElse(Option.empty)),
        comment = comment,
        properties = properties,
        allowExisting = allowExisting,
        replace = replace)

    case view @ ShowViews(CurrentNamespace, pattern, output) =>
      if (ViewUtil.isViewCatalog(catalogManager.currentCatalog)) {
        ShowUCViews(ResolvedNamespace(catalogManager.currentCatalog, catalogManager.currentNamespace),
          pattern, output)
      } else {
        view
      }

    case ShowViews(UnresolvedNamespace(CatalogAndNamespace(catalog, ns), _), pattern, output)
      if ViewUtil.isViewCatalog(catalog) =>
      ShowUCViews(ResolvedNamespace(catalog, ns), pattern, output)

    // needs to be done here instead of in ResolveViews, so that a V2 view can be resolved before the Analyzer
    // tries to resolve it, which would result in an error, saying that V2 views aren't supported
    case u@UnresolvedView(ResolvedView(resolved), _, _, _) =>
      ViewUtil.loadView(resolved.catalog, resolved.identifier)
        .map(_ => ResolvedUCView(resolved.catalog.asViewCatalog, resolved.identifier))
        .getOrElse(u)
  }

  private def isTempView(nameParts: Seq[String]): Boolean = {
    catalogManager.v1SessionCatalog.isTempView(nameParts)
  }

  private object ResolvedIdent {
    def unapply(unresolved: UnresolvedIdentifier): Option[ResolvedIdentifier] = unresolved match {
      case UnresolvedIdentifier(nameParts, true) if isTempView(nameParts) =>
        None

      case UnresolvedIdentifier(CatalogAndIdentifier(catalog, ident), _) if ViewUtil.isViewCatalog(catalog) =>
        Some(ResolvedIdentifier(catalog, ident))

      case _ =>
        None
    }
  }

  private object ResolvedView {
    def unapply(identifier: Seq[String]): Option[ResolvedUCView] = identifier match {
      case nameParts if isTempView(nameParts) =>
        None

      case CatalogAndIdentifier(catalog, ident) if ViewUtil.isViewCatalog(catalog) =>
        ViewUtil.loadView(catalog, ident).flatMap(_ => Some(ResolvedUCView(catalog.asViewCatalog, ident)))

      case _ =>
        None
    }
  }
}
