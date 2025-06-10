package io.transwarp.holodesk.spark

import io.transwarp.holodesk.{JavaHolodeskUniqValSectionColInfo, JavaRowSetSplitHelper, JavaScanHelper, JavaTaskAttachment}
import io.transwarp.holodesk.core.options.ReadOptions
import io.transwarp.holodesk.spark.utils.{ConverterUtils, HolodeskCoreFastSerde}
import io.transwarp.shiva2.shiva.holo.SplitContext
import io.transwarp.tddms.gate.TddmsEnv
import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType

case class HolodeskPartition(localHConf: Map[String, String],
                             idx: Int,
                             tableName: String,
                             requiredSchema: StructType,
                             readOptions: ReadOptions,
                             columns: Array[String],
                             columnsDataTypes: Array[Int],
                             neededColumns: Array[Boolean],
                             options: Map[String, String],
                             splitContexts: Array[SplitContext],
                             attachment: JavaTaskAttachment,
                             singleValSection: Array[JavaHolodeskUniqValSectionColInfo],
                             metaIdx2OutIdx: Map[Int, Int]) extends Partition with Logging {

  override def index: Int = idx

  private def getConf: Configuration = {
    val conf = new Configuration()
    localHConf.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  def getScanIterator: Iterator[Row] = {
    val colNums = columns.length
    TddmsEnv.init(getConf)
    val shivaClient = TddmsEnv.getHoloShiva2Client()
    val holoClient = shivaClient.newHoloClient()
    val rowSets = JavaRowSetSplitHelper.getRowSetsFromSplitContexts(shivaClient, splitContexts)
    val iter = JavaScanHelper.getIter(shivaClient, holoClient, tableName, readOptions, rowSets, attachment)

    val holodeskCoreFastSerde = new HolodeskCoreFastSerde(colNums, columnsDataTypes, null)
    holodeskCoreFastSerde.init()

    val rowConverter = ConverterUtils.createConverterToSparkSQL(
      holodeskCoreFastSerde, columnsDataTypes, singleValSection, requiredSchema, neededColumns, metaIdx2OutIdx)

    new Iterator[Row] {

      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): Row = {
        val record = iter.next
        rowConverter(record).asInstanceOf[GenericRow]
      }
    }

  }

}
