package cn.com.bigdata123.spark.plugin.catalog.hive.read

import cn.com.bigdata123.spark.plugin.catalog.hive.V2Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType

case class HiveFileFormatReadBuilder (
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    table: V2Table)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): Scan = HiveFileScan(sparkSession, fileIndex, readDataSchema(), readPartitionSchema(), table)
}