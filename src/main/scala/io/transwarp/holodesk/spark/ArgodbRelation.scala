package io.transwarp.holodesk.spark

import io.transwarp.holodesk.core.common.{Dialect, ReadMode, ScannerType}
import io.transwarp.holodesk.core.predicate.combinations.OrPredicatesList
import io.transwarp.holodesk.core.predicate.primitive.PrimitivePredicate
import io.transwarp.holodesk.{JavaDbConnectionConf, JavaDbTableMeta, JavaFilterGenerator, JavaHolodeskCoreReadOptionsHelper, JavaHolodeskUniqValSectionColInfo, JavaPredicateWrapper, JavaRowSetSplitHelper, JavaSGTable, JavaTable}
import io.transwarp.holodesk.spark.consts.ConfigKeys
import io.transwarp.holodesk.spark.utils.{ConverterUtils, PartitionPruner, SparkUtils, StargateNodeBuilder}
import io.transwarp.holodesk.transaction.shiva2.TransactionUtilsShiva2
import io.transwarp.shiva2.shiva.bulk.Transaction
import io.transwarp.shiva2.shiva.client.ShivaClient
import io.transwarp.shiva2.shiva.holo.{HoloClient, Table}
import io.transwarp.tddms.gate.TddmsEnv
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Partition, SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import java.util
import scala.collection.JavaConversions.{asScalaBuffer, asScalaSet, mapAsScalaMap}
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, mapAsJavaMapConverter}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class ArgodbRelation(sparkSession: SparkSession, options: Map[String, String])
  extends BaseRelation with PrunedFilteredScan with Logging {

  val conf: SparkConf = SparkEnv.get.conf
  @transient private val hiveConf = SparkUtils.hiveConf
  TddmsEnv.init(hiveConf)

  private val fullTableName: String = options(ConfigKeys.DATABASE_TABLE_NAME)

  private val dialect: String = if (options.contains(ConfigKeys.DIALECT)) options(ConfigKeys.DIALECT)
  else conf.get(ConfigKeys.DIALECT, "ORACLE")

  private val columnPrunnerEnabled: Boolean = if (options.contains(ConfigKeys.ENABLE_COLUMN_PRUNER))
    options(ConfigKeys.ENABLE_COLUMN_PRUNER).toBoolean
  else conf.get(ConfigKeys.ENABLE_COLUMN_PRUNER, "true").toBoolean

  private val filterPushDownEnabled: Boolean = if (options.contains(ConfigKeys.ENABLE_FILTER_PUSHDOWN))
    options(ConfigKeys.ENABLE_FILTER_PUSHDOWN).toBoolean
  else conf.get(ConfigKeys.ENABLE_FILTER_PUSHDOWN, "true").toBoolean

  private val partitionPruneEnabled: Boolean = if (options.contains(ConfigKeys.ENABLE_PARTITION_PRUNE))
    options(ConfigKeys.ENABLE_PARTITION_PRUNE).toBoolean
  else conf.get(ConfigKeys.ENABLE_PARTITION_PRUNE, "true").toBoolean

  private var isRowkeyTable: Boolean = if (options.contains(ConfigKeys.ROWKEY_TABLE))
    options(ConfigKeys.ROWKEY_TABLE).toBoolean else false

  @transient private var dbTableMeta: JavaDbTableMeta = _

  private var singleValColumnNames: Array[String] = if (options.contains(ConfigKeys.SINGLE_VALUE_PARTITION_COLUMN_NAMES)) {
    options(ConfigKeys.SINGLE_VALUE_PARTITION_COLUMN_NAMES).split(",")
  } else null

  val tableName: String = {
    if (StringUtils.isNotBlank(JavaDbConnectionConf.getArgoHost)) {
      dbTableMeta = new JavaDbTableMeta(options.asJava, fullTableName)
      singleValColumnNames = dbTableMeta.getSingleValSectionColumnNames
      isRowkeyTable = dbTableMeta.isRowkeyTable
      logInfo(s"[HolodeskSourceReader] getting real table name: ${dbTableMeta.getRealDbName}  isRowkeyTable: $isRowkeyTable.")
      dbTableMeta.getRealDbName
    } else {
      logWarning("ArgoDB server info is not defined. please ensure 'dbtablename' is the real table name in ArgoDB.")
      fullTableName
    }
  }

  private val INCEPTOR_URL = JavaDbConnectionConf.getUrl
  private val userName = if (StringUtils.equalsIgnoreCase(JavaDbConnectionConf.getAuth, "ldap")) {
    options.getOrElse(ConfigKeys.SPARK_ARGODB_LDAP_USER, JavaDbConnectionConf.getLdapUser)
  } else {
    "hive"
  }
  private val passwd = if (StringUtils.equalsIgnoreCase(JavaDbConnectionConf.getAuth, "ldap")) {
    options.getOrElse(ConfigKeys.SPARK_ARGODB_LDAP_PASSWD, JavaDbConnectionConf.getLdapPassword)
  } else {
    "123456"
  }

  @transient private lazy val shivaClient: ShivaClient = TddmsEnv.getHoloShiva2Client
  @transient private lazy val holoClient: HoloClient = shivaClient.newHoloClient()
  @transient private lazy val shivaTable: Table = holoClient.openTable(tableName)

  private val holodeskTable = new JavaTable(shivaTable)

  private lazy val realSchema: StructType = {
    // reader返回结果的schema信息,包含单值分区列
    if (options.contains("schema")) {
      val schemaStr = options("schema")
      StructType(
        schemaStr.split(",").map(col => StructField(col.split(":")(0),
          ConverterUtils.toDataType(col.split(":")(1))))
      )
    } else {
      if (StringUtils.isNotBlank(JavaDbConnectionConf.getArgoHost)) {
        val columns = dbTableMeta.getJavaStructType
        val fields = new ArrayBuffer[StructField]
        for (col <- columns.asScala) {
          fields.append(StructField(col.getColumnName, ConverterUtils.toDataType(col.getDataType.toLowerCase()), true)
            .withComment(col.getComment))
        }
        StructType(fields.toArray)
      } else {
        logWarning("Using storage data type. Strongly recommend to use ArgoDB type.")
        ConverterUtils.toSqlType(holodeskTable.getColumns)
      }
    }
  }

  private var requestedSchema: StructType = _

  override def schema: StructType = if (requestedSchema != null) requestedSchema else realSchema


  private var neededColumns: Array[Boolean] = _

  private def pruneColumns(requiredColumns: Array[String]): Unit = {
    logInfo("[ARGODB_SPARK] realSchema " + realSchema.treeString)
    neededColumns = new Array[Boolean](realSchema.fields.length).map(_ => true)
    logInfo(s"[ARGODB_SPARK] requiredColumns: [${requiredColumns.mkString(",")}]")
    if (columnPrunnerEnabled) {
      val neededColumnNames = requiredColumns
      for (i <- realSchema.fields.indices) {
        if (!neededColumnNames.contains(realSchema(i).name.toLowerCase())) {
          neededColumns(i) = false
        }
      }
    }
    logInfo(s"[ARGODB_SPARK] NeededColumns: [${neededColumns.mkString(",")}]")

    val schemaBuffer = new ArrayBuffer[StructField]()
    for (i <- realSchema.fields.indices) {
      if (neededColumns(i)) {
        schemaBuffer.append(realSchema.fields(i))
      }
    }
    requestedSchema = StructType(schemaBuffer.toArray)
  }

  private var filters: Array[Filter] = _
  private var singlePartitionFilters: Array[Filter] = _
  private val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()
  private var orPredicates: OrPredicatesList = _
  private var primitivePredicate: PrimitivePredicate = _

  private def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    if (filters.isEmpty) {
      filters
    } else {
      singlePartitionFilters = if (singleValColumnNames != null) {
        filters.filter(filter => (filter.references.toSet & singleValColumnNames.toSet).nonEmpty)
      } else null

      if (filterPushDownEnabled) {
        // 过滤非单值分区的filter
        val nonPartitionFilters = if (singleValColumnNames != null) {
          filters.filter(filter => (filter.references.toSet & singleValColumnNames.toSet).isEmpty)
        } else filters

        val filterOptions = Map("" -> "")
        val (sgNode, supportedFilters, unsupportedFilters) = StargateNodeBuilder.createSGNode(filterOptions, realSchema, nonPartitionFilters)
        this.supportedFilters.appendAll(supportedFilters)
        logInfo(s"The push down filters: ${supportedFilters.mkString(",")}")

        val columnTypeMap = new util.LinkedHashMap[String, String]()
        for (field <- realSchema.fields) {
          columnTypeMap.put(field.name, StargateNodeBuilder.getSGColumnTypeStr(field.dataType))
        }
        val sgTable = new JavaSGTable(columnTypeMap)

        val holoDialect = dialect match {
          case "ORACLE" => Dialect.ORACLE
          case "DB2" => Dialect.DB2
          case "TD" => Dialect.TD
          case _ => throw new RuntimeException("Unsupported Dialect")
        }

        val predicateWrapper = new JavaPredicateWrapper()
        val filterCheck = JavaFilterGenerator.generatePredicateWrapper(predicateWrapper, sgTable, sgNode, holoDialect,
          holodeskTable.isPerformanceTable)

        orPredicates = predicateWrapper.getOrPredicates
        if (holodeskTable.isPerformanceTable) {
          primitivePredicate = predicateWrapper.getPrimitivePredicate
        }

        if (singlePartitionFilters != null) {
          unsupportedFilters.appendAll(singlePartitionFilters)
        }
        // filter 存在冲突时，orPredicates is null
        if (orPredicates == null) {
          logInfo("No filter can push down.")
          filters
        } else if (!filterCheck) {
          // such as doubleCol != 44.22d
          logInfo("Some filter can't push down, return all filters.")
          filters
        } else {
          logInfo(s"Filter push down success, unsupportedFilters: $unsupportedFilters")
          unsupportedFilters.toArray
        }
      } else {
        filters
      }
    }
  }

  private def genHolodeskNeededColumn(neededColumns: Array[Boolean]): Array[Boolean] = {
    val holodeskNeededColumns = neededColumns.clone()
    if (supportedFilters.nonEmpty) {
      val references = supportedFilters.flatMap(_.references).toSet
      for (i <- realSchema.fields.indices) {
        if (!neededColumns(i) && references.contains(realSchema.fields(i).name)) {
          holodeskNeededColumns(i) = true
        }
      }
    }
    // 裁剪单值分区列
    if (singleValColumnNames != null) {
      val start = realSchema.fields.length - singleValColumnNames.length
      holodeskNeededColumns.slice(0, start)
    } else {
      holodeskNeededColumns
    }
  }

  @throws[Exception]
  private def beginTransaction(shivaClient: ShivaClient, readOnlyTx: Boolean): Transaction = {
    val transaction = if (readOnlyTx) shivaClient.newBulkROTransaction
    else shivaClient.newBulkTransaction
    transaction.setWaitForCommitFinish(true)
    transaction.setWaitCommitFinishTimeoutS(100)
    transaction.setWaitLockTimeoutS(-1)
    val status = transaction.begin
    if (!status.ok) throw new RuntimeException("Begin insert transaction failed, error:" + status)
    transaction
  }

  private def configurationToMap(conf: Configuration): Map[String, String] = {
    conf.iterator().asScala.map { entry =>
      entry.getKey -> entry.getValue
    }.toMap
  }

  private val localHConfMap: Map[String, String] = configurationToMap(hiveConf)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    pruneColumns(requiredColumns)
    pushFilters(filters)

    val sections = if (dbTableMeta != null && partitionPruneEnabled) {
      if (!dbTableMeta.getSingleValSectionNames.isEmpty) {
        // 单值分区过滤
        val totalSections = dbTableMeta.getSingleValSectionNames
        logInfo(s"Total sections: ${totalSections}")
        val singleValSectionMap = dbTableMeta.getSingleValSectionMap
        val start = realSchema.fields.length - singleValSectionMap.head._2.length
        val partitionFields = realSchema.fields.slice(start, realSchema.fields.length)
        val prunedPartitions = PartitionPruner.prunePartitions(partitionFields, singleValSectionMap, singlePartitionFilters)
        logInfo(s"After prune partition sections: ${prunedPartitions.keySet}, " +
          s"prune partitions number ${totalSections.size - prunedPartitions.keySet.size}")
        prunedPartitions.keySet
      } else if (!dbTableMeta.getRangeSectionNames.isEmpty) {
        // 范围分区过滤
        val totalSections = dbTableMeta.getRangeSectionNames
        logInfo(s"Total sections: ${totalSections}")
        val rangeSectionMap = dbTableMeta.getRangeSectionMap
        val rangeColumnNames = rangeSectionMap.head._2.map(_.getPartColumnName.toLowerCase())
        val rangeFields = realSchema.fields.filter(field => rangeColumnNames.contains(field.name.toLowerCase()))
        logInfo(s"rangeFields: ${rangeFields.mkString(",")}, filters = ${filters.mkString(",")}")
        val prunedPartitions = PartitionPruner.pruneRangePartitions(rangeFields, rangeSectionMap, filters)
        logInfo(s"After prune partition sections: ${prunedPartitions.keySet}, " +
          s"prune partitions number ${totalSections.size - prunedPartitions.keySet.size}")
        prunedPartitions.keySet
      } else {
        shivaTable.getPublicSections
      }
    } else {
      shivaTable.getPublicSections
    }

    // neededColumns is null if not call pruneColumns
    if (neededColumns == null) {
      neededColumns = new Array[Boolean](realSchema.fields.length).map(_ => true)
    }

    val metaIdx2OutIdxMutable = mutable.Map[Int, Int]()
    for (i <- requiredColumns.indices) {
      val queryColumnName = requiredColumns(i)
      for (j <- realSchema.fields.indices) {
        val schemaColumnName = realSchema(j).name
        if (StringUtils.equalsIgnoreCase(queryColumnName, schemaColumnName)) {
          metaIdx2OutIdxMutable(j) = i
        }
      }
    }
    val metaIdx2OutIdx = metaIdx2OutIdxMutable.toMap

    val holodeskNeededColumns = genHolodeskNeededColumn(neededColumns)
    logInfo(s"HolodeskNeededColumns: ${holodeskNeededColumns.mkString(",")}")

    val transaction = beginTransaction(shivaClient, true)
    val handler = TransactionUtilsShiva2.getMultiSectionReadTransactionHandler(transaction, tableName, sections)
    val distribution = handler.getDistribution
    TransactionUtilsShiva2.commitTransaction(transaction)
    isRowkeyTable = shivaTable.hasHashBucket && dbTableMeta.isRowkeyTable

    if (isRowkeyTable) {
      val bucketFieldName = shivaTable.getHashColumnNames
      holodeskTable.setRowKeyColumn(bucketFieldName.map(_.toUpperCase).toArray)
      if (holodeskNeededColumns != null) {
        holodeskTable.getRowKeyIndex.foreach(holodeskNeededColumns(_) = true)
      }
    }

    val isPerformanceTable = holodeskTable.isPerformanceTable
    val readOptions = if (isPerformanceTable) {
      val rOptions = JavaHolodeskCoreReadOptionsHelper.getReadOptions(
        holodeskTable, holodeskNeededColumns, orPredicates, primitivePredicate)
      rOptions.setNeedLoadInternalRowKey(false)
      rOptions
    } else {
      JavaHolodeskCoreReadOptionsHelper.getReadOptions(holodeskTable, holodeskNeededColumns, orPredicates)
    }
    readOptions.setPreferReadMode(ReadMode.RowMode)
    readOptions.setIsPerformanceTable(isPerformanceTable)

    val columns = holodeskTable.getColumns
    val columnsDataTypes = holodeskTable.getColumnDataTypes

    val partitionListBuffer = new ListBuffer[Partition]
    var idx = -1
    sections.foreach { section =>
      val singleValSection = if (dbTableMeta != null && !dbTableMeta.getSingleValSectionMap.isEmpty) {
        dbTableMeta.getSingleValSectionMap.get(section)
      } else {
        new Array[JavaHolodeskUniqValSectionColInfo](0)
      }
      val rowSetsGroup = JavaRowSetSplitHelper.splitRowSetsToBuckets(distribution, section)

      val partitions = rowSetsGroup.map(
        rowSets => {
          idx += 1
          HolodeskPartition(
            localHConfMap,
            idx = idx,
            tableName = tableName,
            requiredSchema = schema,
            readOptions = readOptions,
            columns = columns.map(_.getColumnName),
            columnsDataTypes = columnsDataTypes,
            neededColumns = neededColumns,
            options = options,
            splitContexts = rowSets.getSplitContexts,
            attachment = rowSets.getJavaTaskAttachment,
            singleValSection = singleValSection,
            metaIdx2OutIdx
          )
        }
      )
      partitionListBuffer.appendAll(partitions)
    }

    new createRDD(sparkSession.sparkContext, partitionListBuffer.toArray)
  }


  override def sqlContext: SQLContext = sparkSession.sqlContext

}
