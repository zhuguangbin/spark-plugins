package cn.com.bigdata123.spark.sql.parser

import cn.com.bigdata123.spark.sql.command.{ShowCatalogsCommand, UseCatalogCommand}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class AddonSparkSqlAstBuilder(session: SparkSession, delegate: ParserInterface) extends AddonSqlBaseBaseVisitor[AnyRef] with Logging {
  import ParserUtils._

  override def visitSingleStatement(ctx: AddonSqlBaseParser.SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    ctx.statement().accept(this).asInstanceOf[LogicalPlan]
  }

  override def visitShowCatalogs(ctx: AddonSqlBaseParser.ShowCatalogsContext): LogicalPlan = withOrigin(ctx) {
    ShowCatalogsCommand()
  }

  override def visitUseCatalog(ctx: AddonSqlBaseParser.UseCatalogContext): AnyRef = withOrigin(ctx) {
    UseCatalogCommand(ctx.catalog.getText)
  }

}
