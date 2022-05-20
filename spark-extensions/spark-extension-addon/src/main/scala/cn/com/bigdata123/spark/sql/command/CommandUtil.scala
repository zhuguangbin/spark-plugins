package cn.com.bigdata123.spark.sql.command

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.InternalBridge.SESSION_CATALOG_NAME

object CommandUtil {
  def allCatalogs(ss: SparkSession): Seq[String] = {
    SESSION_CATALOG_NAME +: ss.sparkContext.getConf
      .getAllWithPrefix("spark.sql.catalog.")
      .map(f => f._1)
      .filterNot(_.contains("."))
      .distinct
      .toSeq
      .sorted
  }
}
