package com.teradata.bigdata.carefree

import java.util.Calendar

import com.teradata.bigdata.carefree.bean.{MarkTable, UserLocationInfo}
import com.teradata.bigdata.carefree.utils.SparkConfig
import com.teradata.bigdata.util.kafka.KafkaProperties
import com.teradata.bigdata.util.spark.BroadcastWrapper
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Project:
  * @Description: 一经逍遥 上传用户每5分钟最后的信令位置，同时关联码表找出基站的经纬度
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/3/003 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object CarefreeStreamingApplication extends TimeFuncs {
  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 3600000L
  val classStr = "CarefreeStreamingApplication"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName(classStr)
      .enableHiveSupport()
      .getOrCreate()

    val sparkConfig = new SparkConfig

    sparkConfig.setConf(sparkSession.conf)

    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(300))
    val sqlContext = sparkSession.sqlContext

    //================码表=====================
    def getMarkTable: DataFrame = {
      val markHdfsPath = "hdfs://nmsq/user/b_yz_app_xy/xiaoyao/tb_lac_cell_info.txt"
      val markRDD: RDD[String] = sc.textFile(markHdfsPath)
      val markRow = markRDD
        .map(_.split("\\|", -1)).map(line => MarkTable(
        line(0),  //lac_id
        line(1),  //cell_id
        line(2),  //经度
        line(3),  //维度
        line(5)   //基站所属地市
      ))

      val markTable = sparkSession.createDataFrame(markRow)
      markTable
    }

    //================码表=====================
    val broadcastMarkTable = BroadcastWrapper[DataFrame](ssc, getMarkTable)

    def updateBroadcast(): Unit = {
      //每隔1分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        broadcastMarkTable.update(getMarkTable, blocking = true)
        //        println("MarkTable:" + broadcastMarkTable.value.count())
        lastTime = toDate
      }
    }

    val kafkaProperties = new KafkaProperties()
    val topics = Array(kafkaProperties.integrationTopic)
    val brokers = kafkaProperties.kafkaBrokers.mkString(",")
    val groupId = classStr

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val kafkaStreams = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(7).nonEmpty) {
        true
      } else{
        false
      }
    }).map(m => {
      //用户IMSI,UserLocationInfo(用户IMSI,终端IMEI,用户号码,所在地市,位置信息来源,业务流程开始时间,TAC   ,小区标识)
      ( m(5),    UserLocationInfo(m(5)   ,m(6)   ,m(7)    ,m(1)   ,m(15)     ,m(10)         ,m(19) ,m(20)))
    }).foreachRDD(rdd =>{
      updateBroadcast
      broadcastMarkTable.value.toDF().createOrReplaceTempView("markTable")
      import sqlContext.implicits._
      rdd.partitionBy(new HashPartitioner(200))
        .map(_._2)
        .groupBy(_.imei)
        .map((kv: (String, Iterable[UserLocationInfo])) =>{
          //取用户最后一条信令数据
          kv._2.toList.sortBy(_.procedure_start_time).reverse.head
        })
        .toDF().createOrReplaceTempView("dataTable")

      val currentTime = timeMillsToDate(System.currentTimeMillis(), "yyyyMMddHHmm")
      val sql = " select  c.msisdn " +
        " ,c.imsi " +
        " ,c.imei " +
        " ,d.city_id " +
        " ,'4' " +
        " ,'' " +
        " ,d.clogiitud " +
        " ,d.clatitude " +
        " ,'' " +
        " ,'' " +
        " ,c.TAC " +
        " ,c.ECI " +
        " ,c.timestamps " +
        " ,''  " +
        " from ( " +
        " select  " +
        " case when substr(msisdn,1,2)='86' then concat('0',substr(msisdn,1,14))  else msisdn end  as msisdn " +
        " ,imsi " +
        " ,imei " +
        " ,local_city " +
        " ,location_source " +
        " ,procedure_start_time as timestamps " +
        " ,hex(cast(TAC as BIGINT)) as TAC " +
        " ,hex(cast(ECI as BIGINT)) as ECI  " +
        " from dataTable " +
        " ) c  " +
        " inner join markTable   d  " +
        "  on c.TAC=d.lac_id  " +
        "  and c.ECI=d.cell_id   "
        sqlContext.sql(sql).map(row =>{
          "0€" +
            row(0) + "€" +      //手机号
            row(1) + "€" +      //imsi
            row(2) + "€" +      //imei
            row(3) + "€" +      //地市代码
            row(4) + "€" +
            row(5) + "€" +
            row(6) + "€" +      //经度
            row(7) + "€" +      //维度
            row(8) + "€" +
            row(9) + "€" +
            row(10) + "€" +     //tac
            row(11) + "€" +     //cell
            row(12) + "€" +     //信令生成时间
            row(13)
        }).rdd.saveAsTextFile(s"hdfs://nmsq/user/b_yz_app_xy/xiaoyao/receive/$currentTime")
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
