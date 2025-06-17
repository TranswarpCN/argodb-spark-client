package io.transwarp.holodesk.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.Utils
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object SparkUtils extends Logging {

  // 注意sparkConext stop后会设置SparkEnv的env为null,所以我们在停止前,保留原sparkenv的引用
  private lazy val env = SparkEnv.get

  @transient lazy val hiveConf = {
    val conf = new Configuration()
    conf.addResource("hive-site.xml")
    conf
  }

}
