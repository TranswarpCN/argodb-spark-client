package io.transwarp.holodesk.spark.utils

import io.transwarp.holodesk.spark.consts.Consts
import io.transwarp.holodesk.{JavaHolodeskRangeSectionColInfo, JavaHolodeskUniqValSectionColInfo}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Not, Or}
import org.apache.spark.sql.types.{ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, TimestampType}

import java.sql.{Date, Timestamp}
import java.util
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.mapAsJavaMapConverter

object PartitionPruner extends Logging {


  def prunePartitions(partitionFields: Array[StructField],
                      partitions: util.Map[String, Array[JavaHolodeskUniqValSectionColInfo]],
                      filters: Array[Filter]): util.Map[String, Array[JavaHolodeskUniqValSectionColInfo]] = {
    if (filters != null) {
      try {
        partitions.filter(entry => {
          val uniqValSections = entry._2
          var matched = true
          for (i <- uniqValSections.indices if matched) {
            matched = testUniqVal(partitionFields(i), uniqValSections(i), filters)
          }
          matched
        }).toMap.asJava
      } catch {
        case e: Exception =>
          logError(s"PartitionPruner failed, due to $e")
          partitions
      }
    } else partitions
  }

  // 范围分区过滤，范围分区范围都是左闭右开
  def pruneRangePartitions(partitionFields: Array[StructField],
                           partitions: util.Map[String, Array[JavaHolodeskRangeSectionColInfo]],
                           filters: Array[Filter]): util.Map[String, Array[JavaHolodeskRangeSectionColInfo]] = {

    if (filters != null) {
      try {
        partitions.filter(entry => {
          val rangeSectionColInfos = entry._2
          var matched = true
          for (i <- rangeSectionColInfos.indices if matched) {
            matched = testRangeVal(partitionFields(i), rangeSectionColInfos(i), filters)
            logInfo(s"${partitionFields(i)}, ${rangeSectionColInfos(i)}, ${matched}")
          }
          matched
        }).toMap.asJava
      } catch {
        case e: Exception =>
          logError(s"PartitionPruner failed, due to $e")
          partitions
      }
    } else partitions
  }


  // 返回true表示保留分区，返回false表示分区需要被过滤
  private def testUniqVal(field: StructField, uniqVal: JavaHolodeskUniqValSectionColInfo, filters: Array[Filter]): Boolean = {
    var matched = true
    for (filter <- filters if matched) {
      var res: Option[Int] = None
      filter match {
        case And(left, right) =>
          matched = testUniqVal(field, uniqVal, Array(left)) && testUniqVal(field, uniqVal, Array(right))
        case Or(left, right) =>
          matched = testUniqVal(field, uniqVal, Array(left)) || testUniqVal(field, uniqVal, Array(right))
        case Not(child) =>
          matched = !testUniqVal(field, uniqVal, Array(child))
        case EqualTo(attribute, value) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
            res = tryCompare(field, partValue, value)
            if (res.isDefined) {
              matched = res.get == 0
            }
          }
        case LessThan(attribute, value) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
            res = tryCompare(field, partValue, value)
            if (res.isDefined) {
              matched = res.get < 0
            }
          }
        case LessThanOrEqual(attribute, value) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
            res = tryCompare(field, partValue, value)
            if (res.isDefined) {
              matched = res.get <= 0
            }
          }
        case GreaterThan(attribute, value) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
            res = tryCompare(field, partValue, value)
            if (res.isDefined) {
              matched = res.get > 0
            }
          }
        case GreaterThanOrEqual(attribute, value) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
            res = tryCompare(field, partValue, value)
            if (res.isDefined) {
              matched = res.get >= 0
            }
          }
        case In(attribute, values) =>
          if (uniqVal.getPartColumnName.equalsIgnoreCase(attribute)) {
            var find = false
            for (value <- values if !find) {
              val partValue = ConverterUtils.partitionColToSparkSQL(field, uniqVal.getValue)
              res = tryCompare(field, partValue, value)
              if (res.isDefined) {
                if (res.get == 0) {
                  find = true
                }
              } else true
            }
            matched = find
          }
        case _ =>
      }
    }
    matched
  }

  private def testRangeVal(field: StructField, range: JavaHolodeskRangeSectionColInfo, filters: Array[Filter]): Boolean = {
    var matched = true
    for (filter <- filters if matched) {
      filter match {
        case And(left, right) =>
          matched = testRangeVal(field, range, Array(left)) && testRangeVal(field, range, Array(right))
        case Or(left, right) =>
          matched = testRangeVal(field, range, Array(left)) || testRangeVal(field, range, Array(right))
        case Not(child) =>
          matched = !testRangeVal(field, range, Array(child))
        case EqualTo(attribute, value) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            val lowValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getLow)
            val lowRes = tryCompare(field, lowValue, value)
            val highValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getHigh)
            val highRes = tryCompare(field, highValue, value)
            if (lowRes.isDefined && highRes.isDefined) {
              matched = lowRes.get <= 0 && highRes.get > 0
            }
          }
        case LessThan(attribute, value) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            val lowValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getLow)
            val lowRes = tryCompare(field, lowValue, value)
            if (lowRes.isDefined) {
              matched = lowRes.get < 0
            }
          }
        case LessThanOrEqual(attribute, value) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            val lowValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getLow)
            val lowRes = tryCompare(field, lowValue, value)
            if (lowRes.isDefined) {
              matched = lowRes.get <= 0
            }
          }
        case GreaterThan(attribute, value) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            val highValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getHigh)
            val highRes = tryCompare(field, highValue, value)
            if (highRes.isDefined) {
              matched = highRes.get > 0
            }
          }
        case GreaterThanOrEqual(attribute, value) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            val highValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getHigh)
            val highRes = tryCompare(field, highValue, value)
            if (highRes.isDefined) {
              matched = highRes.get >= 0
            }
          }
        case In(attribute, values) =>
          if (range.getPartColumnName.equalsIgnoreCase(attribute)) {
            var find = false
            for (value <- values if !find) {
              val lowValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getLow)
              val lowRes = tryCompare(field, lowValue, value)
              val highValue = ConverterUtils.rangPartitionColToSparkSQL(field, range.getHigh)
              val highRes = tryCompare(field, highValue, value)
              if (lowRes.isDefined && highRes.isDefined) {
                find = lowRes.get <= 0 && highRes.get > 0
              }
            }
            matched = find
          }
        case _ =>
      }
    }
    matched
  }


  /**
   * if a is null or b is null return false
   * Try to compare value a and b.
   * If a is greater than b, return 1.
   * If a equals b, return 0.
   * If a is less than b, return -1.
   * If a and b are not comparable, return None.
   */
  private def tryCompare(field: StructField, aa: Any, bb: Any): Option[Int] = {
    if (aa == null && bb != null) {
      return Some(-1)
    }
    if (aa != null && bb == null) {
      return Some(1)
    }
    if (aa == null && bb == null) {
      return Some(0)
    }
    // only check aa, due to aa is partition value, bb is expr value
    if (Consts.RANGE_MINVALUE.equals(aa)) {
      Some(-1)
    } else if (Consts.RANGE_MAXVALUE.equals(aa)) {
      Some(1)
    } else {
      field.dataType match {
        case ByteType | ShortType | IntegerType | LongType =>
          Some(aa.asInstanceOf[Number].longValue().compareTo(bb.asInstanceOf[Number].longValue()))
        case FloatType | DoubleType =>
          Some(aa.asInstanceOf[Number].doubleValue().compareTo(bb.asInstanceOf[Number].doubleValue()))
        case _: DecimalType =>
          Some(aa.asInstanceOf[java.math.BigDecimal].compareTo(bb.asInstanceOf[java.math.BigDecimal]))
        case DateType => Some(aa.asInstanceOf[Date].compareTo(bb.asInstanceOf[Date]))
        case TimestampType => Some(aa.asInstanceOf[Timestamp].compareTo(bb.asInstanceOf[Timestamp]))
        case StringType => Some(aa.asInstanceOf[String].compareTo(bb.asInstanceOf[String]))
        case _ => None
      }
    }
  }


}
