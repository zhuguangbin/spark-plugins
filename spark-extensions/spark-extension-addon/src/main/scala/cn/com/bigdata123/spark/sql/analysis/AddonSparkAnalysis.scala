package cn.com.bigdata123.spark.sql.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object AddonSparkAnalysis {

  def customResolutionRules(): Seq[SparkSession => Rule[LogicalPlan]] =
    Seq(
      session => AddonSparkAnalysis(session)
    )
}

/**
 * Rule for convert the logical plan to command.
 *
 * @param sparkSession
 */
case class AddonSparkAnalysis(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _ => plan
    }
  }
}
