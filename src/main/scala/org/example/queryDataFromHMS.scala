package org.example

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.hadoop.fs.PathFilter
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hudi.DataSourceOptionsHelper
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{col, sqrt}

import java.util
import java.util.{ArrayList, List}
import scala.collection.mutable.ListBuffer


object queryDataFromHMS {

  private val log = LogManager.getLogger(queryDataFromHMS.getClass)

  def main(args: Array[String]): Unit = {


    var queryName = "";
    var queryFields = "";
    var querySql = "";

    if (args.length == 3) {
      queryName = args(0)
      queryFields = args(1)
      querySql = args(2)
    } else {

      throw new RuntimeException("need specify args")
    }

    // for localtest
    //    queryName="test111"


    //注意是目录
    val resultPathDir: String = "/tmp/lyreport/" + queryName

    System.setProperty("HADOOP_USER_NAME", "hive")
    System.setProperty("HADOOP_USER_GROUP_NAME", "hive")

    val spark = SparkSession.builder()
      //for local test
      //      .master("local[*]")
      .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs:///user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://master02:9083")
      .enableHiveSupport()
      .getOrCreate
    spark.sparkContext.hadoopConfiguration.setClass("mapreduce.input.pathFilter.class", classOf[HoodieROTablePathFilter], classOf[PathFilter])

    spark.sparkContext.addSparkListener(new queryCompleteListener(queryName))

    import spark.implicits._
    import scala.collection.JavaConversions._


    // for local test
    //    queryFields = "org_name,org_code,dddd"
    //    querySql = "select order_id,trade_finish_date,discount_money,process_time from lingyun_erp.t_order_info limit 5"
    //    queryFields = "org_code,org_name,ware_code,name,spec,production_cp_name,where_name,unit_name,national_drug_code,made_number,made_date,invalidate,nearly_effective_days,stock_qty,qualified_stock_qty,qualified_qty,warehouse_name,buyer_id,abc_classification,stock_status_code,purchase_price"
    //    querySql = "select /* 报表:批号库存明细 */ first_value(t3.org_name) as org_name, first_value(t3.org_code) as org_code, first_value(t3.org_name) as warehouse_name, first_value(tm.ware_code) as ware_code, /* 旧接口没有 tm.ware_inside_code, tm.qualified_reserved_qty, */ first_value(tm.name) as name, first_value(tm.spec) as spec, first_value(tm.national_drug_code) as national_drug_code, tm.made_number, date_format(first_value(tm.made_date), '%Y-%m-%d') as made_date, date_format(first_value(tm.invalidate), '%Y-%m-%d') as invalidate, round(sum(tm.stock_qty), 2) as stock_qty, round(sum(case when tm.stall_type = 1 then tm.stock_qty else 0 end), 2) as qualified_stock_qty, round(sum(case when tm.stall_type = 1 then tm.stock_qty else 0 end) - sum(case when tm.stall_type = 1 then tm.reserved_qty else 0 end), 4) as qualified_qty, first_value(t15.full_name) buyer_id, first_value(t8.nearly_effective_days) as nearly_effective_days, first_value(t8.abc_classification) as abc_classification, /* tm.measurement_unit_id unit_name, tm.platform_production_origin_place_id where_name, t8.stock_status_code, tm.factory_id production_cp_name, */ /* 字典表补充字段 */ first_value(t16.name) as unit_name, first_value(t17.name) as where_name, first_value(t18.name) as stock_status_code, first_value(t19.production_cp_name) as production_cp_name, /* 门店商品零售价 */ first_value(tm.batch_code) SORT_COLUMN from ( select a.batch_code , a.group_id, a.company_id, a.ware_inside_code, t5.ware_code, t5.name, t5.spec, t5.national_drug_code, a.purchase_price, t5.measurement_unit_id, t5.platform_production_origin_place_id, t5.factory_id, a.made_number, b.stall_type, b.stock_qty, b.reserved_qty, a.delivery_price, a.send_order_dc_org_id, c.made_date, c.invalidate from t_stock_d a inner join  t_stock_c b on a.group_id = b.group_id and a.company_id = b.company_id and a.initial_business_id = b.business_id and a.batch_code = b.batch_code and a.made_number = b.made_number and a.ware_inside_code = b.ware_inside_code and b.company_id in ( 10032 ) left join t_stock_batch_number_info c on c.group_id = a.group_id and c.made_number = a.made_number and c.ware_inside_code = a.ware_inside_code inner join  t_ware_group_base_info t5 on t5.group_id = a.group_id and t5.ware_inside_code = a.ware_inside_code WHERE b.stall_type in (1,2) and a.group_id = 10000 and a.company_id in ( 10032 ) ) tm left join t_org_organization_base t3 on t3.group_id = tm.group_id and t3.id = tm.company_id left join t_org_organization_base t2 on t3.super_dc_id = t2.id left join t_stock_batch_number_info t7 on t7.group_id = tm.group_id and t7.made_number = tm.made_number and t7.ware_inside_code = tm.ware_inside_code left join t_ware_company_base_info t8 on t8.group_id = tm.group_id and t8.company_id = tm.company_id and t8.ware_inside_code = tm.ware_inside_code and t8.company_id in ( 10032 ) left join t_basic_setting_syn_buyer t15 on t8.buyer_id = t15.id /* 字典表补充 */ left join t_basic_setting_tb_code_item t16 ON tm.measurement_unit_id = t16.code_item_id left join t_basic_setting_tb_code_item t17 ON tm.platform_production_origin_place_id = t17.code_item_id left join t_basic_setting_tb_code_item t18 ON t8.stock_status_code = t18.code_item_id left join t_basic_setting_production_cp t19 ON tm.factory_id = t19.production_cp_id group by tm.group_id, tm.company_id, tm.ware_inside_code, tm.made_number ORDER BY org_code desc,ware_code desc,made_number desc"


    log.info("start sql " + querySql)
    //指定 hudi 库
    spark.sql("use lingyun_erp")
    val df = spark.sql(querySql)


    val fieldList = queryFields.split(",")


    val columns = df.columns;
    val result = df.select(fieldList.filter(f => columns.contains(f)).map(m => col(m).cast("String")): _*)

    result.printSchema()
    result.write
      .format("excel")
      .mode(SaveMode.Overwrite)
      .save(resultPathDir)

  }

}
