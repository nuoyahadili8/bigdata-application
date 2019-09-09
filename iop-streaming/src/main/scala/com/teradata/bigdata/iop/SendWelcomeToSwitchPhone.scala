package com.teradata.bigdata.iop

import java.util.{Calendar, Date, Properties}

import com.teradata.bigdata.iop.SendWelcomeToChangePhone.strToDate
import com.teradata.bigdata.iop.utils.IopFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.{LogUtil, TimeFuncs}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * @Project:
  * @Description: 开关机事件
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object SendWelcomeToSwitchPhone extends TimeFuncs
  with Serializable
  with LogUtil
  with IopFuncs{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 120000L
  private val SOURCE_ID = "3"  //活动类型：2【换机事件】 3【开关机事件】 4【位置事件】 5【访问APP事件】
  val classNameStr = "SendWelcomeToSwitchPhone"

  def main(args: Array[String]): Unit = {
    val kafkaProperties = new KafkaProperties
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(30))

    val topics = Array(kafkaProperties.integrationTopic)
    val brokers = kafkaProperties.kafkaBrokers.mkString(",")
    val groupId = classNameStr
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val hbaseUtil = ssc.sparkContext.broadcast(new HbaseUtil)

    // 从hbase获取活动的相关信息 活动ID，（Array(活动运行时间列表)，短信内容，是否为组合事件的标记：默认为false）
    val lacCiActIdListBro = BroadcastWrapper[mutable.HashMap[String, (Array[String], String, Boolean)]](ssc, getActInfo(SOURCE_ID))

    def updateBroadcast() {
      //每隔1分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        lacCiActIdListBro.update(getActInfo(SOURCE_ID), blocking = true)
        lastTime = toDate
      }
    }

    val kafkaStreams: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream[String, String](
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(23).equals("S1-MME") && f(7).nonEmpty) {
        true
      } else{
        false
      }
    }).map(m => {
      // (phone_no, ((start_time ,start_time_long  ,start_time_date)                                  ,procedureType ,phone_no))
      (   m(7),     ((m(11)      ,m(9).toLong      ,strToDate(m(10), "yyyy-MM-dd HH:mm:ss")) ,m(8)          ,m(7)))
    }).foreachRDD(rdd =>{
      if (getIopSalesTimeFlag) {
        if (lacCiActIdListBro.value.size > 0) {
          rdd.partitionBy(new HashPartitioner(40)).foreachPartition(partition =>{
            val lacCiActIdList = lacCiActIdListBro.value

            val targetTopic = "YZ_TD_IOP_SELECT_PHONE"
            val groupActTmpTopic = "YZ_TD_IOP_GROUP_ACT_TMP"

            val conn = hbaseUtil.value.createHbaseConnection

            val partitionPhoneNos = scala.collection.mutable.Set[String]()
            val userData = new ListBuffer[(String, ((String, Long, Date), String, String))]

            // 本批次手机号
            partition.foreach(p => {
              val userCurrentInfo = p._2

              val phoneNo = p._1
              val partitionPhoneNo = getPartitionPhoneNo(phoneNo)
              partitionPhoneNos += partitionPhoneNo
              userData.add((partitionPhoneNo, userCurrentInfo))
            })

            // 对用户记录流程产生时间排序
            val sortedUserData = userData.sortBy(_._2._1._2)

            // 本批次手机号批量在hbase中的查找情况(包括已经营销过的用户信息)
            val userJoindActList: mutable.HashMap[String, List[String]] =
              getUsersActListByGroupFilter(hbaseUtil.value, conn, "b_yz_app_td_hbase:SelectPhoneList3", partitionPhoneNos.toSet)
            // 本批次已经营销用户
            val alreadySaledUser: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
            // 组合事件列表
            var evtEffectPutList: List[(String, String, String, String)] = List()

            sortedUserData.foreach(message =>{
              val partitionPhoneNo = message._1
              val info: ((String, Long, Date), String, String) = message._2   //((start_time, start_time_long, start_time_date), procedureType, phone_no)
              val CurrentStartTime = info._1._3   //信令进入时间
              val userPhoneNo = info._3           //手机号

              if (userJoindActList.contains(partitionPhoneNo)) {
                val actList = userJoindActList(partitionPhoneNo)
                actList.foreach(actId => {
                  if (lacCiActIdList.contains(actId)) {
                    val actInfo: (Array[String], String, Boolean) = lacCiActIdList(actId)
                    val timeSet = actInfo._1.toSet
                    val message = actInfo._2
                    val groupActFlag = actInfo._3
                    timeSet.foreach(StartEndTimeString =>{
                      val startTime = getCurrentTimeByHour(StartEndTimeString.split("-")(0))
                      val endTime = getCurrentTimeByHour(StartEndTimeString.split("-")(1))

                      if (CurrentStartTime.after(startTime) && CurrentStartTime.before(endTime)) {
                        // 如果为组合事件,将数据发送至临时topic
                        if (groupActFlag) {
                          val startTimeLong = info._1._2
                          val tmpMessage = userPhoneNo + "," + actId + "," + SOURCE_ID + "," + startTimeLong

                          kafkaProducer.value.send(groupActTmpTopic, tmpMessage)
                          evtEffectPutList = evtEffectPutList :+ (partitionPhoneNo, actId, SOURCE_ID, startTimeLong.toString)

                        }
                        else if (!alreadySaledUser.contains(partitionPhoneNo)) {
                          // 如果为单一事件，将数据发送至结果topic
                          alreadySaledUser.update(partitionPhoneNo, actId)
                          kafkaProducer.value.send(targetTopic, userPhoneNo + "  " + message)
                        }
                      }
                    })
                  }
                })
              }
            })
            hbaseUtil.value.putResultByKeyList_IOP(conn, "b_yz_app_td_hbase:SelectPhoneList3", alreadySaledUser.toList)
            putGroupActUserCache(hbaseUtil.value, conn, SOURCE_ID, evtEffectPutList)
            if (conn != null) conn.close()
          })
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
