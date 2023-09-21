package org.example

import org.apache.hudi.DataSourceWriteOptions.{MOR_TABLE_TYPE_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object fetchParquetIntoHudi {

  def main(args: Array[String]): Unit = {


    val dbName = "xxxx_erp"

    var loadPath = "/gydl/xxxx_report/t_stock_d"
    var tableName = "t_stock_d"
    var key = "batch_code"
    var preKey = "group_id"
    var haspart = "true"
    var timeKey = "create_time"
    var partKey = "pt_date"


    val testlocal = true
    if (!testlocal) {
      if (args.length == 7) {
        loadPath = args(0)
        tableName = args(1)
        key = args(2)
        preKey = args(3)
        haspart = args(4)
        partKey = args(5)
        timeKey = args(6)
      } else {
        throw new RuntimeException("need specify args")
      }
    }

    val writedPath: String = "/user/hive/warehouse/" + dbName + ".db/" + tableName

    System.setProperty("HADOOP_USER_NAME", "hive")
    // 创建sparkSQL的运行环境
    val conf = new SparkConf()
    if (testlocal) {
      conf.setAppName("fetchParquet").setMaster("local[*]")
    }
    val spark = SparkSession.builder().config(conf)
      // 设置序列化方式：Kryo
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    import spark.implicits._

    val df = spark.read.format("parquet")
      .load(loadPath)


    val newdf = df.withColumn(partKey, to_date(col(timeKey), "yyyy-mm-dd"))
    newdf.printSchema()


    if (haspart == "true") {

      newdf.write
        .format("hudi")
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD.key(), key)
        .option(PRECOMBINE_FIELD.key(), preKey)
        .option(PARTITIONPATH_FIELD.key(), partKey)
        .option(TBL_NAME.key(), "hudi_from_parquet")
        .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .option(OPERATION.key(), "bulk_insert")
        .option("hoodie.index.type", "SIMPLE")
        .option("hoodie.datasource.hive_sync.database", dbName)
        .option("hoodie.datasource.hive_sync.table", tableName)
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.mode", "hms")
        .option("hoodie.datasource.hive_sync.skip_ro_suffix", "true")
        .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://master02:9083")
        .mode(SaveMode.Overwrite)
        .save(writedPath)

    } else {

      newdf.write
        .format("hudi")
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD.key(), key)
        .option(PRECOMBINE_FIELD.key(), preKey)
        .option(TBL_NAME.key(), tableName)
        .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .option(OPERATION.key(), "bulk_insert")
        .option("hoodie.index.type", "SIMPLE")
        .option("hoodie.datasource.hive_sync.database", dbName)
        .option("hoodie.datasource.hive_sync.table", tableName)
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.mode", "hms")
        .option("hoodie.datasource.hive_sync.skip_ro_suffix", "true")
        .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://master02:9083")
        .mode(SaveMode.Overwrite)
        .save(writedPath)


    }


  }
}