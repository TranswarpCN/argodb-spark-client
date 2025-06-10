package io.transwarp.holodesk.spark.utils

import io.transwarp.holodesk.core.result.{ByteArrayColumnResult, RowResult}
import io.transwarp.holodesk.{JavaColumn, JavaHolodeskUniqValSectionColInfo}
import io.transwarp.holodesk.serde.HolodeskType
import io.transwarp.holodesk.spark.consts.Consts
import io.transwarp.holodesk.utils.DecimalUtil
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import java.time.ZoneId

object ConverterUtils {

  @inline
  def toDataType(typeStr: String): DataType = {
    val decimalPattern = """decimal\(([0-9]+),([0-9]+)\)""".r
    typeStr.toLowerCase match {
      case "int" => IntegerType
      case "integer" => IntegerType
      case "tinyint" => ByteType //IntegerType
      case "smallint" => ShortType
      case "bigint" => LongType
      case "long" => LongType
      case "float" => FloatType
      case "boolean" => BooleanType
      case "double" => DoubleType
      case "string" => StringType
      case "date" => DateType
      case "timestamp" => TimestampType
      case decimalPattern(x, y) => DecimalType(x.toInt, y.toInt)
      // varchar2,varchar,char
      case _ => StringType
    }
  }


  def toSqlType(columns: Array[JavaColumn]): StructType = {
    val fields = columns.map(col => {
      col.getColumnDataType match {
        case HolodeskType.BYTE => StructField(col.getColumnName, ByteType, nullable = true)
        case HolodeskType.SHORT => StructField(col.getColumnName, ShortType, nullable = true)
        case HolodeskType.INT => StructField(col.getColumnName, IntegerType, nullable = true)
        case HolodeskType.LONG => StructField(col.getColumnName, LongType, nullable = true)
        case HolodeskType.BOOLEAN => StructField(col.getColumnName, BooleanType, nullable = true)
        case HolodeskType.DOUBLE => StructField(col.getColumnName, DoubleType, nullable = true)
        case HolodeskType.FLOAT => StructField(col.getColumnName, FloatType, nullable = true)
        case HolodeskType.STRING | HolodeskType.CHAR | HolodeskType.VARCHAR | HolodeskType.VARCHAR2 =>
          StructField(col.getColumnName, StringType, nullable = true)
        case _ => StructField(col.getColumnName, StringType, nullable = true)
      }
    })
    StructType(fields)
  }


  def rangPartitionColToSparkSQL(colType: StructField, value: String): Any = {
    val isMinValue = value.equals(Consts.RANGE_MINVALUE)
    val isMaxValue = value.equals(Consts.RANGE_MAXVALUE)

    colType.dataType match {
      case ByteType =>
        if (isMinValue) {
          Byte.MinValue
        } else if (isMaxValue) {
          Byte.MaxValue
        } else value.toByte
      case ShortType =>
        if (isMinValue) {
          Short.MinValue
        } else if (isMaxValue) {
          Short.MaxValue
        } else value.toShort
      case IntegerType =>
        if (isMinValue) {
          Short.MinValue
        } else if (isMaxValue) {
          Short.MaxValue
        } else value.toInt
      case LongType =>
        if (isMinValue) {
          Long.MinValue
        } else if (isMaxValue) {
          Long.MaxValue
        } else value.toLong
      case StringType => value
      case DoubleType =>
        if (isMinValue) {
          Double.MinValue
        } else if (isMaxValue) {
          Double.MaxValue
        } else value.toDouble
      case FloatType =>
        if (isMinValue) {
          Float.MinValue
        } else if (isMaxValue) {
          Float.MaxValue
        } else value.toFloat
      case BooleanType =>
        if (isMaxValue || isMinValue) {
          value
        } else value.toBoolean
      case TimestampType =>
        if (isMaxValue || isMinValue) {
          value
        } else Timestamp.valueOf(value)
      case DateType =>
        if (isMaxValue || isMinValue) {
          value
        } else Date.valueOf(value)
      case DecimalType() =>
        if (isMaxValue || isMinValue) {
          value
        } else BigDecimal(new java.math.BigDecimal(value))
      case _ => value
    }
  }

  def partitionColToSparkSQL(colType: StructField, value: String): Any = {
    colType.dataType match {
      case ByteType => value.toByte
      case ShortType => value.toShort
      case IntegerType => value.toInt
      case LongType => value.toLong
      case StringType => value
      case DoubleType => value.toDouble
      case FloatType => value.toFloat
      case BooleanType => value.toBoolean
      case TimestampType => Timestamp.valueOf(value)
      case DateType => Date.valueOf(value)
      case DecimalType() => BigDecimal(new java.math.BigDecimal(value))
      case _ => value
    }
  }


  def createConverterToSparkSQL(holodeskCoreFastSerde: HolodeskCoreFastSerde,
                                colDataTypes: Array[Int],
                                partCol: Array[JavaHolodeskUniqValSectionColInfo],
                                schema: StructType,
                                neededColumn: Array[Boolean],
                                metaIdx2OutIdx: Map[Int, Int]): AnyRef => AnyRef = {
    (item: AnyRef) => {
      if (item == null) {
        null
      } else {
        val columnResults = item.asInstanceOf[RowResult].getColumns
        val allColLength = neededColumn.count(_ == true)
        val result = new Array[Any](allColLength)
        val realCols = schema.toArray
        var baseCnt = 0
        var outCnt = 0
        var metaCnt = 0
        while (baseCnt < colDataTypes.length) {
          if (neededColumn(baseCnt)) {
            outCnt = metaIdx2OutIdx(baseCnt)
            val isNUll = columnResults(baseCnt) == null || columnResults(baseCnt).asInstanceOf[ByteArrayColumnResult].isNull
            if (isNUll) {
              result(outCnt) = null
            } else {
              realCols(metaCnt).dataType match {
                case ByteType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Byte]
                case ShortType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Short]
                case IntegerType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Int]
                case LongType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Long]
                case StringType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).toString
                case DoubleType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Double]
                case FloatType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Float]
                case BooleanType =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Boolean]
                case TimestampType =>
                  val tempArray = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt)
                  val av = new TimestampWritable()
                  av.setBinarySortable(tempArray.asInstanceOf[Array[Byte]], 0)
                  result(outCnt) = av.getTimestamp
                case DateType =>
                  val holodeskDate = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).asInstanceOf[Long]
                  val date = DateTimeUtils.microsToDays(DateTimeUtils.millisToMicros(holodeskDate), ZoneId.systemDefault())
                  result(outCnt) = DateTimeUtils.toJavaDate(date)
                case DecimalType() =>
                  val tempArray = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt)
                  val tmp = new scala.math.BigDecimal(DecimalUtil.getBigDecimal(tempArray.asInstanceOf[Array[Byte]], 0))
                  val newDecimal = new Decimal()
                  newDecimal.set(tmp)
                  result(outCnt) = newDecimal
                case _ =>
                  result(outCnt) = holodeskCoreFastSerde.getValue(columnResults(baseCnt), baseCnt).toString
              }
            }
            metaCnt += 1
          }
          baseCnt += 1
        }

        if (partCol.nonEmpty) {
          partCol.foreach(col => {
            if (neededColumn(baseCnt)) {
              outCnt = metaIdx2OutIdx(baseCnt)
              if (col.getValue.equals(Consts.DEFAULTPARTITIONNAME)) {
                result(outCnt) = null
              } else if (col.getValue.equals(Consts.DEFAULT_PARTITION_NAME_EMPTYSTR)) {
                result(outCnt) = ""
              } else {
                result(outCnt) = partitionColToSparkSQL(realCols(metaCnt), col.getValue)
              }
              metaCnt += 1
            }
            baseCnt += 1
          })
        }
        new GenericRow(result)
      }
    }
  }


}
