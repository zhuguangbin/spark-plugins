package cn.com.bigdata123.spark.sql.analysis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}

class DataLineageAnalysisRunnable(sparkSession: SparkSession, plan: LogicalPlan) extends Runnable with Logging {

  var sources: Seq[(TableIdentifier, Seq[String])] = Seq()
  var sinks: Seq[(TableIdentifier, Seq[String])] = Seq()

  override def run(): Unit = {

    plan match {
      // only match DataWritingCommand

      case InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, query, _, table, _, outputColumnNames) => doAnalyzeLineage(table, outputColumnNames, query)

      case InsertIntoHiveTable(table, partition, query, _, _, outputColumnNames) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case CreateHiveTableAsSelectCommand(table, query, outputColumnNames, _) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case OptimizedCreateHiveTableAsSelectCommand(table, query, outputColumnNames, _) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case CreateDataSourceTableAsSelectCommand(table, _, query, outputColumnNames) => doAnalyzeLineage(Some(table), outputColumnNames, query)

      case _ => // do nothing

    }
  }

  private def doAnalyzeLineage(targetTable: Option[CatalogTable], outputColumnNames: Seq[String], query: LogicalPlan): Unit = {
    val parser = sparkSession.sessionState.sqlParser
    val analyzer = sparkSession.sessionState.analyzer
    val optimizer = sparkSession.sessionState.optimizer
    val planner = sparkSession.sessionState.planner

    //    val sparkPlan =
    //      if (!plan.analyzed) {
    //        val analyzedPlan = analyzer.executeAndCheck(plan, new QueryPlanningTracker)
    //        val optimizedPlan = optimizer.execute(analyzedPlan)
    //        planner.plan(optimizedPlan).next()
    //      } else {
    //        val optimizedPlan = optimizer.execute(plan)
    //        planner.plan(optimizedPlan).next()
    //      }

    findSources(query)
    sinks = sinks :+ (targetTable.get.identifier, outputColumnNames)

    println(s"Parsing data lineage, sources: [${sources.map(t => t._1.unquotedString + "#" + t._2.mkString(",")).mkString(", ")}], sinks: [${sinks.map(t => t._1.unquotedString + "#" + t._2.mkString(",")).mkString(", ")}]")
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
