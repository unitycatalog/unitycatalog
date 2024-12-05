package io.unitycatalog.spark

import io.unitycatalog.client.model.FunctionInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types.{DataType, StructType}
import scala.collection.JavaConverters._

class ProphecyUnboundFunction(functionInfo: FunctionInfo) extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Any] {
    override def produceResult(input: InternalRow): Any =  {
      null
    }
    override def inputTypes(): Array[DataType] = {
      println(s"inputType = ${inputType.toString()}")
      val inputParams = functionInfo.getInputParams
      if (inputParams != null) {
        println(s"inputParams = ${inputParams.toString}")
        val paramsInfo = inputParams.getParameters

        paramsInfo.asScala.map(x =>
          scala.util.Try(
            DataType.fromDDL(x.getTypeText)
          ).getOrElse(
            DataType.fromJson(x.getTypeJson)
          )
        ).toArray
      } else Array.empty
    } // Replace with actual types
    println(s"functionResponse = ${functionInfo.toString}")
    override def resultType(): DataType = {
      scala.util.Try(
        DataType.fromDDL(functionInfo.getFullDataType)
      ).orElse(
        scala.util.Try(
          DataType.fromDDL(functionInfo.getDataType.getValue)
        )).getOrElse(
        DataType.fromJson(functionInfo.getFullDataType)
      )
    }

    override def name(): String = functionInfo.getName
  }

  override def description(): String = functionInfo.getComment

  override def name(): String = functionInfo.getName
}
