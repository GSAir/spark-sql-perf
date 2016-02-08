/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpch

import scala.sys.process._

import org.apache.spark.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

class Tables(sqlContext: SQLContext, dbgenDir: String, scaleFactor: Int) extends Serializable with Logging {
  import sqlContext.implicits._

  def sparkContext = sqlContext.sparkContext
  val dbgen = s"$dbgenDir/dbgen"

  case class Table(name: String, partitionColumns: Seq[String], dbgenFlag: String, fields: StructField*) {
    val schema = StructType(fields)
    val partitions = 1

    def nonPartitioned: Table = {
      Table(name, Nil, dbgenFlag, fields : _*)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean) = {
      val generatedData = {
        sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
          val localToolsDir = if (new java.io.File(dbgen).exists) {
            dbgenDir
          } else if (new java.io.File(s"/$dbgen").exists) {
            s"/$dbgenDir"
          } else {
            sys.error(s"Could not find dbgen at $dbgen or /$dbgen. Run install")
          }

          val commands = Seq(
            "bash", "-c",
            s"cd $localToolsDir && ./dbgen -T $dbgenFlag -s $scaleFactor")
          println(commands)
          commands.lines
        }
      }

      generatedData.setName(s"$name, sf=$scaleFactor, strings")

      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            col(f.name).cast(f.dataType).as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def useDoubleForDecimal(): Table = {
      val newFields = fields.map { field =>
        val newDataType = field.dataType match {
          case decimal: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, dbgenFlag, newFields:_*)
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text")
      val tempTableName = s"${name}_text"
      data.registerTempTable(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
              |SELECT
              |  $columnString
              |FROM
              |  $tempTableName
              |$predicates
              |DISTRIBUTE BY
              |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          println(s"Pre-clustering with partitioning columns with query $query.")
          logInfo(s"Pre-clustering with partitioning columns with query $query.")
          grouped.write
        } else {
          data.write
        }
      } else {
        // If the table is not partitioned, coalesce the data to a single file.
        data.coalesce(1).write
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      println(s"Generating table $name in database to $location with save mode $mode.")
      logInfo(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createExternalTable(location: String, format: String, databaseName: String, overwrite: Boolean): Unit = {
      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        println(s"Creating external table $name in database $databaseName using data stored in $location.")
        logInfo(s"Creating external table $name in database $databaseName using data stored in $location.")
        sqlContext.createExternalTable(qualifiedTableName, location, format)
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      logInfo(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format)
        .option("delimiter", "|")
        .option("header", "false") // use first line of all files as header
        .option("inferschema", "false") // automatically infer data types
        .schema(schema)
        .load(location).registerTempTable(name)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: String = ""): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (!tableFilter.isEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter(_.name == tableFilter)
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    val withSpecifiedDataType = if (useDoubleForDecimal) {
      tablesToBeGenerated.map(_.useDoubleForDecimal())
    } else {
      tablesToBeGenerated
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/${table.name}.tbl"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues)
    }
  }

  def createExternalTables(location: String, format: String, databaseName: String, overwrite: Boolean, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }

    sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}.tbl"
      table.createExternalTable(tableLocation, format, databaseName, overwrite)
    }
    sqlContext.sql(s"USE $databaseName")
    println(s"The current database has been set to $databaseName.")
    logInfo(s"The current database has been set to $databaseName.")
  }

  def createTemporaryTables(location: String, format: String, tableFilter: String = ""): Unit = {
    val filtered = if (tableFilter.isEmpty) {
      tables
    } else {
      tables.filter(_.name == tableFilter)
    }
    filtered.foreach { table =>
      val tableLocation = s"$location/${table.name}.tbl"
      table.createTemporaryTable(tableLocation, format)
    }
  }

  val tables = Seq(
    Table("part", Nil, "P",
        StructField("p_partkey", IntegerType, true),
        StructField("p_name", StringType, true),
        StructField("p_mfgr", StringType, true),
        StructField("p_brand", StringType, true),
        StructField("p_type", StringType, true),
        StructField("p_size", IntegerType, true),
        StructField("p_container", StringType, true),
        StructField("p_retailprice", DoubleType, true),
        StructField("p_comment", StringType, true)
    ),
    Table("supplier", Nil, "s",
        StructField("s_suppkey", IntegerType, true),
        StructField("s_name", StringType, true),
        StructField("s_address", StringType, true),
        StructField("s_nationkey", IntegerType, true),
        StructField("s_phone", StringType, true),
        StructField("s_acctbal", DoubleType, true),
        StructField("s_comment", StringType, true)
    ),
    Table("partsupp", Nil, "S",
        StructField("ps_partkey", IntegerType, true),
        StructField("ps_suppkey", IntegerType, true),
        StructField("ps_availqty", IntegerType, true),
        StructField("ps_supplycost", DoubleType, true),
        StructField("ps_comment", StringType, true)
    ),
    Table("customer", Nil, "c",
        StructField("c_custkey", IntegerType, true),
        StructField("c_name", StringType, true),
        StructField("c_address", StringType, true),
        StructField("c_nationkey", IntegerType, true),
        StructField("c_phone", StringType, true),
        StructField("c_acctbal", DoubleType, true),
        StructField("c_mktsegment", StringType, true),
        StructField("c_comment", StringType, true)
    ),
    Table("orders", Nil, "O",
        StructField("o_orderkey", IntegerType, true),
        StructField("o_custkey", IntegerType, true),
        StructField("o_orderstatus", StringType, true),
        StructField("o_totalprice", DoubleType, true),
        StructField("o_orderdate", DateType, true),
        StructField("o_orderpriority", StringType, true),
        StructField("o_clerk", StringType, true),
        StructField("o_shippriority", IntegerType, true),
        StructField("o_comment", StringType, true)
    ),
    Table("lineitem", Nil, "L",
        StructField("l_orderkey", IntegerType, true),
        StructField("l_partkey", IntegerType, true),
        StructField("l_suppkey", IntegerType, true),
        StructField("l_linenumber", IntegerType, true),
        StructField("l_quantity", DoubleType, true),
        StructField("l_extendedprice", DoubleType, true),
        StructField("l_discount", DoubleType, true),
        StructField("l_tax", DoubleType, true),
        StructField("l_returnflag", StringType, true), // Char
        StructField("l_linestatus", StringType, true),
        StructField("l_shipdate", DateType, true),
        StructField("l_commitdate", DateType, true),
        StructField("l_receiptdate", DateType, true),
        StructField("l_shipinstruct", StringType, true),
        StructField("l_shipmode", StringType, true),
        StructField("l_comment", StringType, true)
    ),
    Table("nation", Nil, "n",
        StructField("n_nationkey", IntegerType, true),
        StructField("n_name", StringType, true),
        StructField("n_regionkey", IntegerType, true),
        StructField("n_comment", StringType, true)
    ),
    Table("region", Nil, "r",
        StructField("r_regionkey", IntegerType, true),
        StructField("r_name", StringType, true),
        StructField("r_comment", StringType, true)
    )
  )
}
