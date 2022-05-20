package cn.com.bigdata123.spark.sql.command

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.StringType


case class ShowCatalogsCommand() extends RunnableCommand {
  override def output: Seq[Attribute] = {
    AttributeReference("catalog", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
      CommandUtil.allCatalogs(sparkSession).map(Row(_))
  }
}
