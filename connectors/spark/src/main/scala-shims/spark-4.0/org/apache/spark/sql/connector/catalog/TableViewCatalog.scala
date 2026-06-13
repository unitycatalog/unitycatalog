/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.apache.spark.sql.connector.catalog

/**
 * Stub for the real `TableViewCatalog` interface that Spark 4.2 introduced in
 * [SPARK-52729](https://github.com/apache/spark/pull/51419). On Spark 4.0 and 4.1 the
 * real interface does not exist on the classpath, so the connector's
 * `with TableViewCatalog` mixin would fail to verify at class-loading time. This stub
 * lets the connector compile against all three Spark versions while leaving the
 * actual view-side implementation to per-version `ViewSupport` shims.
 *
 * On Spark 4.2 the real Spark interface is on the classpath, so this stub file is
 * absent under `scala-shims/spark-4.2/`. The Delta connector uses the same
 * fake-interface trick for `SupportsV1OverwriteWithSaveAsTable` (see Delta's
 * `spark/src/main/scala-shims/spark-4.0/SupportsV1OverwriteWithSaveAsTable.scala`).
 */
trait TableViewCatalog extends TableCatalog
