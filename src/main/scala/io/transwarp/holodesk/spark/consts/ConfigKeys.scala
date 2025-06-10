package io.transwarp.holodesk.spark.consts

object ConfigKeys {

  val DATABASE_TABLE_NAME = "dbtablename"
  val DIALECT = "spark.holodesk.dialect"

  val ENABLE_COLUMN_PRUNER = "spark.holodesk.column.pruner.enabled"
  val ENABLE_FILTER_PUSHDOWN = "spark.holodesk.filter.pushdown.enabled"
  val ENABLE_PARTITION_PRUNE = "spark.holodesk.partition.prune.enabled"

  val ROWKEY_TABLE = "holodesk.rowkey"

  val SINGLE_VALUE_PARTITION_COLUMN_NAMES = "single.value.partition.column.names"

  val SPARK_ARGODB_LDAP_USER = "spark.argodb.ldap.user"
  val SPARK_ARGODB_LDAP_PASSWD = "spark.argodb.ldap.password"

}
