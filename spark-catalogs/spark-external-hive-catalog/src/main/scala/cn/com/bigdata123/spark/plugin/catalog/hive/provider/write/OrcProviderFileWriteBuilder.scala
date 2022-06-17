package cn.com.bigdata123.spark.plugin.catalog.hive.provider.write

import cn.com.bigdata123.spark.plugin.catalog.hive.V2Table
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.orc.OrcConf.{COMPRESS, MAPRED_OUTPUT_SCHEMA}
import org.apache.orc.mapred.OrcStruct
import org.apache.spark.sql.InternalBridge.{AtomicType, OrcOutputWriter, UserDefinedType, orcFileFormat}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.orc.{OrcOptions, OrcUtils}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class OrcProviderFileWriteBuilder(
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean,
    info: LogicalWriteInfo,
    table: V2Table) extends ProviderFileWriteBuilder(paths, formatName, supportsDataType, info, table) {

  override def prepareWrite(sqlConf: SQLConf, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val orcOptions = new OrcOptions(options, sqlConf)

    val conf = job.getConfiguration

    conf.set(MAPRED_OUTPUT_SCHEMA.getAttribute, orcFileFormat.getQuotedSchemaString(dataSchema))

    conf.set(COMPRESS.getAttribute, orcOptions.compressionCodec)

    conf.asInstanceOf[JobConf]
      .setOutputFormat(classOf[org.apache.orc.mapred.OrcOutputFormat[OrcStruct]])

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        val compressionExtension: String = {
          val name = context.getConfiguration.get(COMPRESS.getAttribute)
          OrcUtils.extensionsForCompressionCodecNames.getOrElse(name, "")
        }

        compressionExtension + ".orc"
      }
    }
  }
}

object OrcProviderFileWriteBuilder {
  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case st: StructType => st.forall { f => supportsDataType(f.dataType) }
    case ArrayType(elementType, _) => supportsDataType(elementType)
    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)
    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)
    case _ => false
  }
}


