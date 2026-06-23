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

package org.apache.spark.sql.catalyst.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedNamespace}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{CreateUCView, DropUCView, LogicalPlan, ShowUCViews}
import org.apache.spark.sql.classic.Strategy
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.SparkPlan

case class ExtendedDataSourceV2Strategy(spark: SparkSession) extends Strategy with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case DropUCView(ResolvedIdentifier(viewCatalog: ViewCatalog, ident), ifExists) =>
      DropV2ViewExec(viewCatalog, ident, ifExists) :: Nil

    case CreateUCView(ResolvedIdentifier(viewCatalog: ViewCatalog, ident), queryText, query,
    columnAliases, columnComments, queryColumnNames, comment, properties, allowExisting, replace, _) =>
      CreateV2ViewExec(
        catalog = viewCatalog,
        ident = ident,
        queryText = queryText,
        columnAliases = columnAliases,
        columnComments = columnComments,
        queryColumnNames = queryColumnNames,
        viewSchema = query.schema,
        comment = comment,
        properties = properties,
        allowExisting = allowExisting,
        replace = replace) :: Nil

    case ShowUCViews(ResolvedNamespace(catalog: ViewCatalog, namespace, _), pattern, output) =>
      ShowV2ViewsExec(output, catalog, namespace, pattern) :: Nil

    case _ => Nil
  }

}
