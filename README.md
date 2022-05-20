# spark-plugins

This is an addon spark plugin to enhance spark. 

## Features:

1. Support multiple Hive data source

Spark can only connect one Hive nowdays. This `AddonSparkSQLExtension` extends spark, you can mount multiple hive as V2ExternalCatalog and all the tables of that as V2Table (ExternalTable), the original default hive sees as SessionCatalog.

2. enhance Spark SQL syntax

We import CATALOG as a first class citizen in Spark SQL.

```
SHOW CATALOGS
USE CATALOG <catalog name>
```

## Senarios and Usecases

1. current DW based on old hive version (eg. 1.x ), and new DW is built on latest new version(2.x or 3.x).

You can use SparkSQL to federate both of them. New DW is built from scratch, old DW data is also accessible. This is a more smooth migration way.

2. Multi IDC Hive federation

IDC-A/IDC-B/IDC-C has their own hive/hadoop seperately. You can mount these hive metastores to three spark catalogs.

These three catalogs are seen in one Spark App. Thus, you can union/join these datasources in one sql job, or you can insert into local IDC hive select from remote IDC hive.

## How-To

1. compile and build 
```
mvn clean package
```

When success, the addon extension jar is located at `spark-extensions/spark-extension-addon/target/spark-extension-addon-0.1-SNAPSHOT.jar`

2. spark configuration 

update `spark-defaults.conf`, an example as follows:

```
# use AddonSparkSQLExtension to extend spark sql capacity
spark.sql.extensions                            cn.com.bigdata123.spark.sql.AddonSparkSQLExtension

# add extension jar to driver classpath and yarn dist jar
spark.driver.extraClassPath        /data/spark/lib/spark-extension-addon-0.1-SNAPSHOT.jar
spark.yarn.dist.jars               file:///data/spark/lib/spark-extension-addon-0.1-SNAPSHOT.jar

# default catalog
spark.sql.defaultCatalog			spark_catalog

# default session catalog configuration
spark.sql.hive.metastore.version                2.3.7
spark.sql.hive.metastore.jars                   builtin 

# local hive 1.2.1 as external catalog, which named local_hive1
spark.sql.catalog.local_hive1						cn.com.bigdata123.spark.plugin.catalog.hive.V2ExternalCatalog
# put local hive 1.2.1 hive-site.xml to SPARK_CONF_DIR, rename it to hive-site.{CATALOG_NAME}.xml
spark.sql.catalog.local_hive1.hive-site-file				hive-site.local_hive1.xml
spark.sql.catalog.local_hive1.spark.sql.hive.metastore.version  		1.2.1
spark.sql.catalog.local_hive1.spark.sql.hive.metastore.jars             	path
spark.sql.catalog.local_hive1.spark.sql.hive.metastore.jars.path        	file:///opt/hive-1.2.1/lib/*.jar,file:///opt/hadoop/share/hadoop/common/*.jar,file:///opt/hadoop/share/hadoop/common/lib/*.jar,file:///opt/hadoop/share/hadoop/hdfs/*.jar,file:///opt/hadoop/share/hadoop/hdfs/lib/*.jar,file:///opt/hadoop/share/hadoop/yarn/*.jar,file:///opt/hadoop/share/hadoop/yarn/lib/*.jar,file:///opt/hadoop/share/hadoop/mapreduce/*.jar,file:///opt/hadoop/share/hadoop/mapreduce/lib/*.jar
# you can build an assemble jar and put it to hdfs instead of a bunch of local jar files
#spark.sql.catalog.local_hive1.spark.sql.hive.metastore.jars.path		hdfs://localhost:8020/data/spark/lib/hive-metastore-libs/hive-metastore-libs-1.2.1-assemble.jar

```

## Usage

```
$ spark-sql 
22/05/20 12:27:55 WARN Utils: Your hostname, C02GM22AMD6T resolves to a loopback address: 127.0.0.1; using 192.168.1.40 instead (on interface en0)
22/05/20 12:27:55 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/05/20 12:27:58 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
Spark master: yarn, Application Id: application_1653020495427_0004
// default session catalog
spark-sql> show databases;
default
hive_db
hudi_db
iceberg_db
test
Time taken: 1.763 seconds, Fetched 5 row(s)
spark-sql> use hive_db;
Time taken: 0.053 seconds
spark-sql> show tables;
Time taken: 0.071 seconds
spark-sql> use test;
Time taken: 0.024 seconds
spark-sql> show tables;
test    hello_hive    false
test    hello_spark    false
Time taken: 0.062 seconds, Fetched 2 row(s)

// show all mounted catalogs
spark-sql> show catalogs;
spark_catalog
local_hive1
Time taken: 0.022 seconds, Fetched 2 row(s)

// change to local_hive1 catalog
spark-sql> use catalog local_hive1;
Time taken: 0.033 seconds
spark-sql> show databases;
default
test1
Time taken: 0.585 seconds, Fetched 2 row(s)
spark-sql> use test1;
Time taken: 0.026 seconds
spark-sql> show tables;
Time taken: 0.053 seconds

// create a target table in hive1(local_hive1 catalog) which data if computed from hive2 (spark_catalog catalog)
spark-sql> create table hello_hive1_table as select * from spark_catalog.test.hello_hive;
22/05/20 12:30:03 WARN V2ExternalCatalog: A Hive serde table will be created as there is no table provider specified. You can set spark.sql.legacy.createHiveTableByDefault to false so that native data source table will be created instead.
22/05/20 12:30:04 WARN SessionState: METASTORE_FILTER_HOOK will be ignored, since hive.security.authorization.manager is set to instance of HiveAuthorizerFactory.
Time taken: 5.519 seconds
spark-sql> select * from hello_hive1_table;
1    Bob
2    Alice
3    Lucy
Time taken: 0.632 seconds, Fetched 3 row(s)

// change back to spark_catalog (default catalog)
spark-sql> use catalog spark_catalog;
Time taken: 0.097 seconds
spark-sql> use test;
Time taken: 0.021 seconds
spark-sql> show tables;
test    hello_hive    false
test    hello_spark    false
Time taken: 0.043 seconds, Fetched 2 row(s)

// create a target table in hive2(default catalog) which data is computed from hive1 (local_hive1 catalog)
spark-sql> create table hello_hive2_table as select * from local_hive1.test1.hello_hive1_table;
22/05/20 12:31:08 WARN ResolveSessionCatalog: A Hive serde table will be created as there is no table provider specified. You can set spark.sql.legacy.createHiveTableByDefault to false so that native data source table will be created instead.
22/05/20 12:31:09 ERROR KeyProviderCache: Could not find uri with key [dfs.encryption.key.provider.uri] to create a keyProvider !!
Time taken: 1.312 seconds
spark-sql> set spark.sql.legacy.createHiveTableByDefault=false;
spark.sql.legacy.createHiveTableByDefault    false
Time taken: 0.022 seconds, Fetched 1 row(s)
spark-sql> create table hello_hive2_table as select * from local_hive1.test1.hello_hive1_table;
Error in query: Table test.hello_hive2_table already exists. You need to drop it first.
spark-sql> drop table hello_hive2_table;
Time taken: 0.464 seconds
spark-sql> create table hello_hive2_table as select * from local_hive1.test1.hello_hive1_table;
Time taken: 1.301 seconds
spark-sql> show tables;
test    hello_hive    false
test    hello_hive2_table    false
test    hello_spark    false
Time taken: 0.039 seconds, Fetched 3 row(s)
spark-sql> select * from hello_hive2_table;
1    Bob
2    Alice
3    Lucy
Time taken: 0.31 seconds, Fetched 3 row(s)
spark-sql> 

// see which catalog and database you are now in
spark-sql> show current namespace;
spark_catalog    test
Time taken: 0.033 seconds, Fetched 1 row(s)

// change to another catalog, see it again
spark-sql> use catalog local_hive1;
Time taken: 0.02 seconds
spark-sql> show current namespace;
local_hive1    default
Time taken: 0.014 seconds, Fetched 1 row(s)
spark-sql> use test1;
Time taken: 0.018 seconds
spark-sql> show current namespace;
local_hive1    test1
Time taken: 0.016 seconds, Fetched 1 row(s)
spark-sql> 

// union the two hive datasource in a query
spark-sql> select * from (select * from spark_catalog.test.hello_hive2_table union all select * from local_hive1.test1.hello_hive1_table);
1    Bob
2    Alice
3    Lucy
1    Bob
2    Alice
3    Lucy
Time taken: 2.893 seconds, Fetched 6 row(s)
spark-sql>

// explore more，like join etc.
```
