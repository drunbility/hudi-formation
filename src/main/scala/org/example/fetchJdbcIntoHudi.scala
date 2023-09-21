package org.example

import org.apache.hudi.DataSourceWriteOptions.{MOR_TABLE_TYPE_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_timestamp, to_date}
import org.apache.spark.sql.{SaveMode, SparkSession}

object fetchJdbcIntoHudi {


  def main(args: Array[String]): Unit = {


    //直接修改
    val testlocal = false
    val tidbIp = "10.233.3.xxx:xxxx"
    val tidbName = "dev_read"
    val tidbPass = "!xxxxx"

    // 脚本传参
    var dbName = "h3_trade"
    var tableName = "t_order_info"
    var key = "order_id"
    var preKey = "group_id"
    var haspart = "true"
    var timeKey = "trade_finish_time"
    var partKey = "trade_finish_date"

    if (!testlocal) {

      if (args.length == 7) {
        dbName = args(0)
        tableName = args(1)
        key = args(2)
        preKey = args(3)
        haspart = args(4)
        timeKey = args(5)
        partKey = args(6)

      } else {
        throw new RuntimeException("need specify args")
      }

    }


    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("HADOOP_USER_GROUP_NAME", "hive")


    val conf = new SparkConf()
    if (testlocal) {
      //for local test
      conf.setAppName("inert hudi async hive").setMaster("local[*]")

    }


    val spark = SparkSession.builder().config(conf)
      .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://master02:9083")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val hudiDb = "uat_lingyun_erp"

    val hudiPath = "/user/hive/realtime_data/" + hudiDb + ".db/" + tableName
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://" + tidbIp +
        "/h3_report_bi?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowMultiQueries=true&zeroDateTimeBehavior=round&autoReconnect=true&wait_timeout=36000&max_execution_time=3000000")
      .option("dbtable", dbName + "." + tableName)
      .option("user", tidbName)
      .option("password", tidbPass)
      .option("numPartitions", "200")
      .option("pushDownOffset","true")
      .option("partitionColumn", "create_time")
      .option("lowerBound", "2000-01-01 00:00:00")
      .option("upperBound", "2023-08-25 09:08:02")
      .load().cache()

    if (haspart == "true") {
      val newdf = jdbcDF
        .withColumn(partKey, to_date(col(timeKey), "yyyy-mm-dd"))
        .withColumn("process_time", current_timestamp())
        .withColumn("event_time", current_timestamp()).cache()
      //        .withColumnRenamed(timeKey, partKey).cache()


      //      println(newdf.count())
      //      newdf.show(100)

      newdf.write
        .format("hudi")
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD.key(), key)
        .option(PRECOMBINE_FIELD.key(), preKey)
        .option(PARTITIONPATH_FIELD.key(), partKey)
        .option(TBL_NAME.key(), tableName)
        .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .option(OPERATION.key(), "upsert")
        .option("hoodie.index.type", "SIMPLE")
        .option("hoodie.datasource.hive_sync.database", hudiDb)
        .option("hoodie.datasource.hive_sync.table", tableName)
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.mode", "hms")
        .option("hoodie.datasource.hive_sync.skip_ro_suffix", "true")
        .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://master02:9083")
        .mode(SaveMode.Overwrite)
        .save(hudiPath)
    } else {

      val newdf = jdbcDF
        .withColumn("process_time", current_timestamp())
        .withColumn("event_time", current_timestamp()).cache()

      newdf.write
        .format("hudi")
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD.key(), key)
        .option(PRECOMBINE_FIELD.key(), preKey)
        .option(TBL_NAME.key(), tableName)
        .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .option(OPERATION.key(), "upsert")
        .option("hoodie.index.type", "SIMPLE")
        .option("hoodie.datasource.hive_sync.database", hudiDb)
        .option("hoodie.datasource.hive_sync.table", tableName)
        .option("hoodie.datasource.hive_sync.enable", "true")
        .option("hoodie.datasource.hive_sync.mode", "hms")
        .option("hoodie.datasource.hive_sync.skip_ro_suffix", "true")
        .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://master02:9083")
        .mode(SaveMode.Overwrite)
        .save(hudiPath)

    }


  }
}
