package io.unitycatalog.spark.compat

import org.apache.spark.sql.catalyst.analysis.CTESubstitution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.CatalogPlugin

object SparkViewCompatibility {

  private lazy val missingCatalogViewsAbilityErrorMethod = {
    val errorsClass = Class.forName("org.apache.spark.sql.errors.QueryCompilationErrors")
    errorsClass.getMethods
      .find(_.getName == "missingCatalogViewsAbilityError")
      .orElse(errorsClass.getMethods.find(_.getName == "missingCatalogAbilityError"))
      .getOrElse {
        throw new IllegalStateException(
          "Unsupported QueryCompilationErrors view catalog error API")
      }
  }

  def missingCatalogViewsAbilityError(plugin: CatalogPlugin): Throwable = {
    missingCatalogViewsAbilityErrorMethod.getName match {
      case "missingCatalogViewsAbilityError" =>
        missingCatalogViewsAbilityErrorMethod
          .invoke(null, plugin)
          .asInstanceOf[Throwable]
      case "missingCatalogAbilityError" =>
        missingCatalogViewsAbilityErrorMethod
          .invoke(null, plugin, "views")
          .asInstanceOf[Throwable]
      case other =>
        throw new IllegalStateException(
          s"Unsupported QueryCompilationErrors view catalog error API: $other")
    }
  }

  def substituteCtesAndOrdinals(plan: LogicalPlan): LogicalPlan = {
    val withCtes = CTESubstitution.apply(plan)
    try {
      val substituteClass =
        Class.forName("org.apache.spark.sql.catalyst.analysis.SubstituteUnresolvedOrdinals$")
      val module = substituteClass.getField("MODULE$").get(null)
      substituteClass
        .getMethod("apply", classOf[LogicalPlan])
        .invoke(module, withCtes)
        .asInstanceOf[LogicalPlan]
    } catch {
      case _: ClassNotFoundException | _: NoSuchMethodException => withCtes
    }
  }
}
