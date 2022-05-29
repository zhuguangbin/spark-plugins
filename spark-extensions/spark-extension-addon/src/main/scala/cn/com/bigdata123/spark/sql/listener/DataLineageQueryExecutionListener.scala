package cn.com.bigdata123.spark.sql.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.util.QueryExecutionListener

class DataLineageQueryExecutionListener extends QueryExecutionListener with Logging{
  var sources: Seq[(TableIdentifier, Seq[String])] = Seq()
  var sinks: Seq[(TableIdentifier, Seq[String])] = Seq()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val user = qe.sparkSession.sparkContext.sparkUser
    val appName = qe.sparkSession.sparkContext.appName
    val appId = qe.sparkSession.sparkContext.applicationId
    val appAttemptId = qe.sparkSession.sparkContext.applicationAttemptId
    val analyzedPlan = qe.analyzed

    logInfo(s"user: $user, appName: $appName, appId: $appId, appAttemptId: $appAttemptId")

    analyzedPlan match {
      // only match DataWritingCommand

      case InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, query, _, table, _, outputColumnNames) => doAnalyzeLineage(table, outputColumnNames, query)

      case InsertIntoHiveTable(table, partition, query, _, _, outputColumnNames) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case CreateHiveTableAsSelectCommand(table, query, outputColumnNames, _) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case OptimizedCreateHiveTableAsSelectCommand(table, query, outputColumnNames, _) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case CreateDataSourceTableAsSelectCommand(table, _, query, outputColumnNames) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case _ => // do nothing

    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    logError(s"Exception during function $funcName, ${exception.toString}")
  }

  private def doAnalyzeLineage(targetTable: Option[CatalogTable], outputColumnNames: Seq[String], query: LogicalPlan): Unit = {

    findSources(query)
    sinks = sinks :+ (targetTable.get.identifier, outputColumnNames)

    logInfo(s"Parsing data lineage, sources: [${sources.map(t => t._1.unquotedString + "#" + t._2.mkString(",")).mkString(", ")}], sinks: [${sinks.map(t => t._1.unquotedString + "#" + t._2.mkString(",")).mkString(", ")}]")
  }

  private def findSources(query: LogicalPlan): Unit = {

    query match {
      case LogicalRelation(base, output, catalogTable, isStreaming) => {
        sources = sources :+ (catalogTable.get.identifier, output.map(attr => attr.qualifiedName))
      }
      case _ => if (query.children.nonEmpty) query.children.foreach(q => findSources(q))
    }
  }
}
