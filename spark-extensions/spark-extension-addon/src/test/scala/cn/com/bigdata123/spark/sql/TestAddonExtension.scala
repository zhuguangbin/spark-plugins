package cn.com.bigdata123.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestAddonExtension extends FunSuite with BeforeAndAfterAll {

  val conf = new SparkConf()
    .set("spark.sql.catalog.local_hive1", "cn.com.bigdata123.spark.plugin.catalog.hive.V2ExternalCatalog")
    .set("spark.sql.queryExecutionListeners", "cn.com.bigdata123.spark.sql.listener.DataLineageQueryExecutionListener")

  protected lazy val spark: SparkSession = SparkSession.builder()
    .appName("TestSparkApp")
    .master("local[*]")
    .config(conf)
    .enableHiveSupport()
    .withExtensions(new AddonSparkSQLExtension)
    .getOrCreate()


  test("test for multicatalog parser") {
    val df = spark.sql("show catalogs")
    println(df.queryExecution.logical)
    println(df.queryExecution.analyzed)
    println(df.queryExecution.optimizedPlan)
    println(df.queryExecution.sparkPlan)
    println(df.queryExecution.executedPlan)

    assert(df.collect().size === 2)
    assert(df.collect().map(r => r.getString(0)).toSeq.contains("spark_catalog"))
    assert(df.collect().toSet ===
      Set(Row("spark_catalog"), Row("local_hive1")))
  }

  test("test use catalog") {
    spark.sql("use catalog local_hive1")

    assert(spark.sessionState.catalogManager.currentCatalog.name() === "local_hive1")
  }

  test("data lineage") {
//    spark.sql("insert into test.hello_hive select id,name from test.hello_spark")
    spark.sql("insert into test.hello_hive2_table select c.id,c.name from (select a.id,b.name from test.hello_hive a join test.hello_spark b on a.id=b.id) c;")
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }
}
