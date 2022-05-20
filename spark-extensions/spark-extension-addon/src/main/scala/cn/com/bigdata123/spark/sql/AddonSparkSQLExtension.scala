package cn.com.bigdata123.spark.sql

import cn.com.bigdata123.spark.sql.analysis.AddonSparkAnalysis
import cn.com.bigdata123.spark.sql.parser.AddonSparkSqlParser
import org.apache.spark.sql.SparkSessionExtensions

class AddonSparkSQLExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {

    extensions.injectParser { (session, parser) =>
      new AddonSparkSqlParser(session, parser)
    }

    AddonSparkAnalysis.customResolutionRules().foreach { rule =>
      extensions.injectResolutionRule { session =>
        rule(session)
      }
    }
  }
}
