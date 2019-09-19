package com.teradata.bigdata.iop.export

import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.spark.SparkConfig
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/18/018 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object IopExportHbaseToLocal {

  val classNameStr = "IopExportHbaseToLocal"

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: argsInfo <HBase tableName> <Local dir path> <current date>")
      System.exit(1)
    }

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config((new SparkConfig).getConf)
      .appName(classNameStr)
      .getOrCreate()

    val hbaseUtil = new HbaseUtil
    val hbaseConfig = hbaseUtil.getConf()
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "")

  }

}
