package cn.com.bigdata123.spark.sql.command

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.CatalogNotFoundException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

case class UseCatalogCommand(catalog: String) extends RunnableCommand with Logging {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogs = CommandUtil.allCatalogs(sparkSession)
    if (!catalogs.contains(catalog)) {
      throw new CatalogNotFoundException(s"spark.sql.catalog.$catalog is not defined")
    }
    sparkSession.sessionState.catalogManager.setCurrentCatalog(catalog)
    Seq.empty[Row]
  }
}
