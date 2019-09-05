package com.teradata.bigdata.kafka.integration

import com.teradata.bigdata.util.kafka.KafkaProperties
import com.teradata.bigdata.util.spark.SparkConfig
import com.teradata.bigdata.util.tools.TimeFuncs
import com.xiaoleilu.hutool.util.StrUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @Project:
  * @Description:  将kafka的O_DPI_LTE_MME,O_DPI_MC_LOCATIONUPDATE_2G,O_DPI_MC_LOCATIONUPDATE_3G整合到Topic【YZ_APP_TD_234G_DPI_DATA】
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/26/026 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object IntegrationKafkaTopicWithStructStreaming extends TimeFuncs{

  def main(args: Array[String]): Unit = {
    val kafkaProperties = new KafkaProperties()

    val kafkaServers = kafkaProperties.kafkaBrokers.mkString(",")

    val sourceTopic = kafkaProperties.sourceTopic.mkString(",")

    val targetTopic = kafkaProperties.integrationTopic

    val spark: SparkSession = SparkSession
      .builder()
      .config((new SparkConfig).getConf)
      .appName("IntegrationKafkaTopicWithStructStreaming")
      .getOrCreate()

    val lines: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", "latest")
      .option("kafkaConsumer.pollTimeoutMs", "5000")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")          //将整行记录转为String

    import spark.implicits._
//      0 所在省
//      1 所在地市
//      2 归属省
//      3 归属地市
//      4 用户漫游类型
//      5 用户IMSI
//      6 终端IMEI
//      7 用户号码
//      8 流程类型编码
//      9 业务流程开始时间Long
//      10业务流程开始时间SSS
//      11业务流程开始时间
//      12业务流程结束时间
//      13经度
//      14纬度
//      15位置信息来源
//      16流程状态
//      17终端用户的IPv4地址
//      18终端用户的IPv6地址
//      19TAC
//      20小区标识
//      21对端小区的TAC
//      22对端小区的ECI
//      23数据来源类型
//      24手机号末尾2位+手机号11位（手机号分区）
    val dfMme = lines
      .mapPartitions(
        partition => {
          var resultList: List[String] = List()
          partition.foreach((line: Row) => {
            var arr = line.toString().split("\\|", -1)
            arr.size match {
              case 211 => {
                val startTimeLong = arr(13).toLong
                val startTimeSSS = timeMillsToDate(startTimeLong, "yyyy-MM-dd HH:mm:ss.SSS")
                val startTime = startTimeSSS.substring(0, 19)
                val phoneNo = arr(11).replaceAll("^86", "")
                var phoneNoPartition = ""
                if (!StrUtil.isEmpty(phoneNo)){
                  phoneNoPartition = phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
                }
                val dataType = "S1-MME"
                val resLine = arr(1) + "," + arr(2) + "," + arr(3) + "," + arr(4) + "," + arr(5) + "," + arr(9) + "," + arr(10) + "," + phoneNo + "," + arr(12) + "," + arr(13) + "," + startTimeSSS + "," + startTime + "," + arr(14) + "," + arr(15) + "," + arr(16) + "," + arr(17) + "," + arr(19) + "," + arr(37) + "," + arr(38) + "," + arr(43) + "," + arr(44) + "," + arr(45) + "," + arr(46) + "," + dataType + "," + phoneNoPartition
                resultList = resLine :: resultList
              }
              case 75 => {
                arr = line.toString().split("\\|", -1)
                val dataType = "MC-3G"
                deal23g(arr,dataType)
              }
              case 74 => {
                arr = line.toString().split("\\|", -1)
                val dataType = "MC-2G"
                deal23g(arr,dataType)
              }
              case _ => None
            }

            def deal23g(arr: Array[String],dataType:String): Unit = {
              val startTimeSSS = arr(0)
              val startTime = startTimeSSS.substring(0, 19)
              val startTimeLong = date2TimeStamp(startTime, format = "yyyy-MM-dd HH:mm:ss").toLong
              val lac = arr(29)
              val cell = arr(30)
              val imsi = arr(21)
              val imei = arr(22)
              val phoneNo = arr(18).replaceAll("^86", "")
              var phoneNoPartition = ""
              if (!StrUtil.isEmpty(phoneNo)){
                phoneNoPartition = phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
              }
              val endTime = arr(1)
              //        val local_city = ""
              val roam_type = arr(73)
              val owner_province = arr(71)
              val owner_city = arr(72)
              val resLine = "" + "," + "" + "," + owner_province + "," + owner_city + "," + roam_type + "," + imsi + "," + imei + "," + phoneNo + "," + "" + "," + startTimeLong.toString + "," + startTimeSSS + "," + startTime + "," + endTime + "," + "" + "," + "" + "," + "" + "," + "" + "," + "" + "," + "" + "," + lac + "," + cell + "," + "" + "," + "" + "," + dataType + "," + phoneNoPartition
              resultList = resLine :: resultList
            }
          })
          resultList.toIterator
        }).toDF()

    val query = dfMme
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", targetTopic)
      .start()

    query.awaitTermination()
  }

}
