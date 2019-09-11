package com.teradata.bigdata.carefree

import java.util.Calendar

import com.teradata.bigdata.carefree.bean.UserInfo
import com.teradata.bigdata.carefree.utils.{SparkConfig, UserStayDurationFunc}
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.KafkaProperties
import com.teradata.bigdata.util.spark.BroadcastWrapper
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
  * @Project:
  * @Description: 用户在基站下驻留的时长
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/11/011 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object UserStayDurationStreaming extends TimeFuncs with UserStayDurationFunc {

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 3600000L * 24
  val classNameStr = "UserStayDurationStreaming"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("UserStayDurationStreaming")
      .enableHiveSupport()
      .getOrCreate()

    val sparkConfig = new SparkConfig
    sparkConfig.setConf(sparkSession.conf)

    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(300))
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val kafkaProperties = new KafkaProperties

    val topics = Array(kafkaProperties.integrationTopic)
    val brokers = kafkaProperties.kafkaBrokers.mkString(",")
    val groupId = classNameStr
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    val kafkaStreams = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    //异常用户的集合（手机号），【二期hive表：TB_TRAVAL_USER_STAYDURATION_MUL_DAY  hdfs://nmdsj133nds/user/zhuhongtao/TB_TRAVAL_USER_STAYDURATION_MUL_DAY/*/*】
    def getUnusualUser(): Set[String] = {
      Set()
    }

    //imsi与phoneNo关系，【二期Hive表：bdoc_dm.TB_OFR_IMSI_ROAM_IN_HIS_TMP】
    def getImsiPhoneNoRelation(): mutable.HashMap[String, String] = {
      new mutable.HashMap[String, String]()
    }


    val unusualUser = BroadcastWrapper[Set[String]](ssc, getUnusualUser)
    val imsiPhoneNoRelation = BroadcastWrapper[Unit](ssc, getImsiPhoneNoRelation)

    def updateDayBroadcast(): Unit = {
      //每隔1天更新异常用户广播变量
      //每隔1天更新号码回填表广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        unusualUser.update(getUnusualUser, blocking = true)
        imsiPhoneNoRelation.update(getImsiPhoneNoRelation, blocking = true)
        //        println("MarkTable:" + broadcastMarkTable.value.count())
        lastTime = toDate
      }
    }

    val filterStream = kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(5).length == 15) {
        true
      } else{
        false
      }
    }).map(m => {
    //(imsi,     UserInfo(ownerProvince ,ownerCity ,localProvince ,localCity ,imei ,tableFlag ,imsi ,phoneNo ,startTime ,lac   ,cell))
      (m(5),     UserInfo(m(2)          ,m(3)      ,m(0)          ,m(1)      ,m(6) ,m(23)     ,m(5) ,m(7)    ,m(10)     ,m(19) ,m(20)))
    }).filter(!_._2.startTime.equals(""))

    filterStream.foreachRDD(rdd => {
      updateDayBroadcast

      //生成用户基站切换的信息
      val hiveExportList: RDD[String] = rdd.partitionBy(new HashPartitioner(400))
        .mapPartitions((partition: Iterator[(String, UserInfo)]) => {
          // 当前信令分布imsi的集合
          var reImsiSet: Set[String] = Set()

          val dataList: Seq[UserInfo] = partition.map((kv: (String, UserInfo)) => {
            reImsiSet = reImsiSet.+(partitionedImsi(kv._1))
            kv._2
          }).toList

          val hbaseUtil = new HbaseUtil
          val conn = hbaseUtil.createHbaseConnection
          //imsi, (lacCell, updTime)
          val lastUserStatusMap: mutable.Map[String, (String, Long)] = getLastUserStatusMap(hbaseUtil, conn, reImsiSet)

          var hiveExportList: List[String] = List()

          dataList.groupBy(_.imsi)    //groupBy改变了数据结构 Map[String, Seq[UserInfo]]
            .foreach(kv => {
            val imsi = kv._1
            val reImsi = partitionedImsi(imsi)

            kv._2.sortBy(_.startTime)  // 时间排序  防止批次乱 分到不同计算单元计算导致结果不准确
              .foreach((info: UserInfo) => {
              val procedureStartTime = info.startTime.toLong
              val currentLacCell = info.lac + "-" + info.cell

              //hbase中包含已经有的用户记录
              if (lastUserStatusMap.contains(reImsi)) {
                val lastStatus: (String, Long) = lastUserStatusMap(reImsi)
                val lacCell = lastStatus._1
                val updTime = lastStatus._2
                if (procedureStartTime >= updTime) {
                  //                      当用户切换位置时：
                  if (!currentLacCell.equals(lacCell)) {
                    //                        关闭上条记录，新增hive记录：
                    //                        1、开始时间为最开始新增的时间
                    //                        2、duration=当前时间与最后更新时间的差值
                    //                        3、结束时间为当前记录时间
                    val finalDuration = procedureStartTime - updTime
                    val procedureEndTime = procedureStartTime

                    val flag ={
                      info.tableFlag match {
                        case "S1-MME" => "1"
                          _ => "2"
                      }
                    }

                    hiveExportList = hiveExportList :+ (timeMillsToDate(procedureEndTime, "yyyyMMdd") + ","
                      + info.phoneNo + ","
                      + info.ownerProvince + ","
                      + info.ownerCity + ","
                      + info.localProvince + ","
                      + info.localCity + ","
                      + imsi + ","
                      + info.imei + ","
                      + info.lac + ","
                      + info.cell + ","
                      + flag + ","   //1:mme4G,2:2G,3:3G,
                      + timeMillsToDate(updTime, "yyyy-MM-dd HH:mm:ss.SSS") + ","
                      + finalDuration / 1000 + ","
                      + timeMillsToDate(procedureEndTime, "yyyy-MM-dd HH:mm:ss.SSS")
                      )
                    //                        更新hbase记录，当前新的laccell,当前新的开始时间
                    val newUpdTime = procedureStartTime
                    val newLacCell = currentLacCell
                    lastUserStatusMap.update(reImsi, (newLacCell, newUpdTime))
                  }
                  //                      当用户没有切换位置：什么也不做
                }
              } else {
                //                    新增记录到hbase，当前laccell,当前开始时间
                val newUpdTime = procedureStartTime
                val newLacCell = currentLacCell
                lastUserStatusMap.update(reImsi, (newLacCell, newUpdTime))
              }
            })
          })

          val hbaseUpdateList: List[(String, String)] = lastUserStatusMap
            .map(kv => (kv._1, getUserCurrentInfoJsonString(kv._2._1, kv._2._2.toString)))
            .toList

          // 更新b_yz_app_td_hbase:UserStayDuration用户在基站驻留时长实时表
          putLastestUserStatusMap(hbaseUtil, conn, hbaseUpdateList)
          if (conn != null) conn.close()
          hiveExportList.toIterator
        })

      //HDFS存储的数据文件load_time_m以5分钟一个
      val (currentDate, currentHour, currentMinute) = getCurrentLoadTime()
      val hdfsHeaderPath = "hdfs://nmsq"
      val currentLoadPathString = "/user/b_yz_app_xy/userStayDurationTmp/" + currentDate + "/" + currentHour + "/" + currentMinute
      val currentLoadPath = new Path(hdfsHeaderPath + currentLoadPathString)
      if (hdfs.exists(currentLoadPath)) {
        hdfs.delete(currentLoadPath, true)
      }

      hiveExportList.saveAsTextFile(currentLoadPathString)

      sparkSession.sql("alter table b_yz_app_xy_hive.TB_TRAVAL_USER_STAYDURATION_REALTIME_DAY add IF NOT EXISTS PARTITION (load_time_d="
        + currentDate + ",load_time_h=" + currentHour + ",load_time_m=" + currentMinute + ")")

      sparkSession.sql("load data inpath '"
        + currentLoadPathString +
        "' overwrite into table b_yz_app_xy_hive.TB_TRAVAL_USER_STAYDURATION_REALTIME_DAY " +
        " partition (load_time_d=" + currentDate + ",load_time_h=" + currentHour + ",load_time_m=" + currentMinute + ")")

      if (hdfs.exists(currentLoadPath)) {
        hdfs.delete(currentLoadPath, true)
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
