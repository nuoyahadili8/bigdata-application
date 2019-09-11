package com.teradata.bigdata.tour

import com.teradata.bigdata.util.spark.SparkConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/10/010 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object App {

  def main(args: Array[String]): Unit = {
    val session: SparkSession =SparkSession.builder().config((new SparkConfig).getConf).appName("App").getOrCreate()

    val config = new HBaseConfiguration()

//    val hbaseContext = new HBaseContext(session.sparkContext, config)
  }
}
