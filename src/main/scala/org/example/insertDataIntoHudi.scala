package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object insertDataIntoHudi {


  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("HADOOP_USER_GROUP_NAME", "hive")


    val conf = new SparkConf().setAppName("test inert").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf)
      .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://master02:9083")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val tableName: String = "hudi_mor_t1"
    val tablePath: String = "/user/hive/warehouse/test.db/parquet_for_hudi"


    import spark.implicits._
    import scala.collection.JavaConversions._

    import org.apache.hudi.QuickstartUtils._

    val generator = new DataGenerator()


    val insertDatas = convertToStringList(generator.generateInserts(100))

    val insertDF = spark.read.json(spark.sparkContext.parallelize(insertDatas, 2).toDS())


    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._


    insertDF.write
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(TBL_NAME.key(), tableName)
      .option(TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
      .option(OPERATION.key(), "upsert")
      .option("hoodie.index.type", "SIMPLE")


      //      .option("hoodie.datasource.hive_sync.jdbcurl", "jdbc:hive2://master02:10000/")
      //      .option("hoodie.datasource.hive_sync.use_jdbc","false")
      //      .option("hoodie.datasource.hive_sync.database", "test")
      //      .option("hoodie.datasource.hive_sync.table", tableName)
      //      .option("hoodie.datasource.hive_sync.enable", "true")
      //      .option("hoodie.datasource.hive_sync.mode", "hms")
      //      .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://master02:9083")
      .mode(SaveMode.Overwrite)
      .save(tablePath)


  }
}
