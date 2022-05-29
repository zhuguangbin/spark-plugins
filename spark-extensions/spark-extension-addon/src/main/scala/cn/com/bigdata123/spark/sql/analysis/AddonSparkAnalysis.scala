package cn.com.bigdata123.spark.sql.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}

import java.util.concurrent.{ExecutorService, Executors}

object AddonSparkAnalysis {

  def customResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => ExampleResolutionRule(session)
    )

  def customCheckRules(): Seq[SparkSession => (LogicalPlan => Unit)] =
    Seq(
//      session => DataLineageCheckRule(session)
    )
}

/**
 * Example Rule for logical plan resolution.
 *
 * @param sparkSession
 */
case class ExampleResolutionRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _ => plan
    }
  }
}

/**
 * Check rule for analysing data lineage, supporting table level and column level lineage
 * @param sparkSession
 */

case class DataLineageCheckRule(sparkSession: SparkSession) extends (LogicalPlan => Unit) {
  val executor: ExecutorService = Executors.newCachedThreadPool();

  override def apply(plan: LogicalPlan): Unit = {
//    println(s"Parsing data lineage, Logical Plan")
    executor.execute(new DataLineageAnalysisRunnable(sparkSession, plan));
  }
}
