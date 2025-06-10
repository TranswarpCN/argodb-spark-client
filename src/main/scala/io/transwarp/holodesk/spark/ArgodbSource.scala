package io.transwarp.holodesk.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class ArgodbSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "holodesk"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ArgodbSource.ensureRegistered(sqlContext.sparkSession)
    ArgodbRelation(sqlContext.sparkSession, parameters)
  }
}


object ArgodbSource {

  private var registered = false

  private def ensureRegistered(spark: SparkSession): Unit = synchronized {
    if (!registered) {
      register(spark)
      registered = true
    }
  }

  // 注册方法
  private def register(spark: SparkSession): Unit = {
    //spark.conf.set("spark.sql.extensions", "io.transwarp.holodesk.spark.ArgodbSource")
  }

}
