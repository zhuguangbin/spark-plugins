/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.sql

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._

object KyuubiSQLConf {

  val INSERT_REPARTITION_BEFORE_WRITE =
    buildConf("spark.sql.optimizer.insertRepartitionBeforeWrite.enabled")
      .doc("Add repartition node at the top of query plan. An approach of merging small files.")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val INSERT_REPARTITION_NUM =
    buildConf("spark.sql.optimizer.insertRepartitionNum")
      .doc(s"The partition number if ${INSERT_REPARTITION_BEFORE_WRITE.key} is enabled. " +
        s"If AQE is disabled, the default value is ${SQLConf.SHUFFLE_PARTITIONS.key}. " +
        "If AQE is enabled, the default value is none that means depend on AQE.")
      .version("1.2.0")
      .intConf
      .createOptional

  val DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM =
    buildConf("spark.sql.optimizer.dynamicPartitionInsertionRepartitionNum")
      .doc(s"The partition number of each dynamic partition if " +
        s"${INSERT_REPARTITION_BEFORE_WRITE.key} is enabled. " +
        "We will repartition by dynamic partition columns to reduce the small file but that " +
        "can cause data skew. This config is to extend the partition of dynamic " +
        "partition column to avoid skew but may generate some small files.")
      .version("1.2.0")
      .intConf
      .createWithDefault(100)

}
