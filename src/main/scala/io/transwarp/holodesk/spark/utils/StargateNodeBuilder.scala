package io.transwarp.holodesk.spark.utils

import io.transwarp.holodesk.{JavaSGNode, JavaSGNodeBuilder, JavaStargateNodeConverter, SgTypeStr}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import java.math.RoundingMode
import java.sql.{Date, Timestamp}
import scala.collection.mutable.ArrayBuffer

object StargateNodeBuilder extends Logging {


  /**
   * 转换filter转换，无法转换的fitler将不会下推
   *
   * @param schema
   * @param filters
   * @return
   */
  def createSGNode(options: Map[String, String], schema: StructType, filters: Array[Filter]):
  (JavaSGNode, ArrayBuffer[Filter], ArrayBuffer[Filter]) = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap

    // 独立转换每一个filter为sgNode
    val sgNodes = filters.map(filter => buildSGNode(options, dataTypeMap, filter))

    val unsupportedFilters = new ArrayBuffer[Filter]
    val supportedFilters = new ArrayBuffer[Filter]
    for (i <- sgNodes.indices) {
      //todo decimal filter can't push down?
      if (sgNodes(i) == null) {
        unsupportedFilters.append(filters(i))
      } else {
        supportedFilters.append(filters(i))
      }
    }
    // 合并sgNode，并且转为OR连接形式
    val supportSGNodes = sgNodes.filter(node => node != null)
    val sgNode = if (supportSGNodes.length > 1) {
      val andChildren = Array[JavaSGNode](supportSGNodes(0), supportSGNodes(1))
      var andNode = JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFAnd", andChildren, "")
      for (i <- 2 until supportSGNodes.length) {
        val andChildren = Array[JavaSGNode](andNode, supportSGNodes(i))
        andNode = JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFAnd", andChildren, "")
      }
      val orNode = JavaStargateNodeConverter.convertSGNode(andNode)
      logInfo(s"Before convert to or is $andNode, after is $orNode")
      if (orNode == null) andNode else orNode
    } else if (sgNodes.length == 1) {
      sgNodes(0)
    } else {
      new JavaSGNode(null)
    }

    (sgNode, supportedFilters, unsupportedFilters)
  }

  private def buildSGNode(options: Map[String, String], dataTypeMap: Map[String, DataType], expression: Filter): JavaSGNode = {
    import org.apache.spark.sql.sources._
    // decimal type filter can't push down
    val containsDecimalType = expression.references.exists(dataTypeMap(_).isInstanceOf[DecimalType])
    if (containsDecimalType) {
      new JavaSGNode(null)
    } else {
      expression match {
        case And(left, right) =>
          val node1 = buildSGNode(options, dataTypeMap, left)
          val node2 = buildSGNode(options, dataTypeMap, right)
          if (node1 != null && node2 != null) {
            val andChildren = Array[JavaSGNode](node1, node2)
            JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFAnd", andChildren, "")
          } else {
            new JavaSGNode(null)
          }
        case Or(left, right) =>
          val node1 = buildSGNode(options, dataTypeMap, left)
          val node2 = buildSGNode(options, dataTypeMap, right)
          if (node1 != null && node2 != null) {
            val orChildren = Array[JavaSGNode](node1, node2)
            JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFOr", orChildren, "")
          } else {
            new JavaSGNode(null)
          }
        case Not(child) =>
          val childNode = Array[JavaSGNode](buildSGNode(options, dataTypeMap, child))
          if (childNode != null) {
            JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFNot", childNode, "")
          } else {
            new JavaSGNode(null)
          }
        case EqualTo(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value)
          val eqChildren = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFEqual", eqChildren, sgType)
        case LessThan(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value)
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFLessThan", children, sgType)
        case LessThanOrEqual(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value)
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFEqualOrLessThan", children, sgType)
        case GreaterThan(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value)
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFGreaterThan", children, sgType)
        case GreaterThanOrEqual(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value)
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFEqualOrGreaterThan", children, sgType)
        case IsNull(attribute) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val children = Array[JavaSGNode](sgColumn)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFNull", children, "")
        case IsNotNull(attribute) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val children = Array[JavaSGNode](sgColumn)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFNotNull", children, "")
        case In(attribute, values) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val constantsNodes = values.map(v => {
            buildConstantNode(sgType, v)
          })
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val children = sgColumn +: constantsNodes
          val types = values.map(v => sgColumn.getSGColumnType)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFIn", children, types)
        case StringStartsWith(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, value + '%')
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFLike", children, "")
        case StringEndsWith(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, '%' + value)
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFLike", children, "")
        case StringContains(attribute, value) =>
          val sgType = getSGColumnTypeStr(dataTypeMap(attribute))
          val sgColumn = JavaSGNodeBuilder.buildSGColumnNode(attribute, sgType)
          val constant = buildConstantNode(sgType, '%' + value + '%')
          val children = Array[JavaSGNode](sgColumn, constant)
          JavaSGNodeBuilder.buildSGFunctionNode("StargateUDFLike", children, "")
        case _ =>
          logWarning(s"Can't build sgNode for filter ${expression.getClass}")
          null
      }
    }
  }

  private def buildConstantNode(sgTypeStr: String, value: Any): JavaSGNode = {
    JavaSGNodeBuilder.buildSGConstantNode(sgTypeStr, value.asInstanceOf[AnyRef])
  }

  def getSGColumnTypeStr(dataType: DataType): String = {
    dataType match {
      case ByteType => SgTypeStr.SGByte
      case ShortType => SgTypeStr.SGShort
      case IntegerType => SgTypeStr.SGInt
      case LongType => SgTypeStr.SGLong
      case StringType => SgTypeStr.SGString
      case DoubleType => SgTypeStr.SGDouble
      case FloatType => SgTypeStr.SGFloat
      case BooleanType => SgTypeStr.SGBoolean
      case TimestampType => SgTypeStr.SGTimestamp
      case DateType => SgTypeStr.SGDate
      case DecimalType() =>
        s"SGDecimal(${dataType.asInstanceOf[DecimalType].precision}" +
          s"#${dataType.asInstanceOf[DecimalType].scale}" +
          s"#${RoundingMode.HALF_UP.name()})"
      case _ => throw new Exception(s"Unsupported $dataType convert to SGColumnType!")
    }
  }


}
