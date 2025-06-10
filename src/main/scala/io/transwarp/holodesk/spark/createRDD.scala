package io.transwarp.holodesk.spark

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class createRDD(sc: SparkContext, parts: Array[Partition]) extends RDD[Row](sc, Nil) {

  override protected def getPartitions: Array[Partition] = parts

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val partition = split.asInstanceOf[HolodeskPartition]
    partition.getScanIterator
  }

}