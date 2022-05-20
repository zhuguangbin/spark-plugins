package org.apache.spark.sql.hive

import org.apache.spark.sql.hive.client.HiveClientImpl

object InternalHiveBridge {
  val hadoopTableReader: HadoopTableReader.type = org.apache.spark.sql.hive.HadoopTableReader
  val hiveClientImpl: HiveClientImpl.type  = org.apache.spark.sql.hive.client.HiveClientImpl
  type FileSinkDesc = org.apache.spark.sql.hive.HiveShim.ShimFileSinkDesc
}

