package com.teradata.bigdata.iop

import java.util.{Calendar, Properties}

import com.teradata.bigdata.iop.utils.IopFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.{LogUtil, TimeFuncs}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Project:
  * @Description: 位置事件
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object SendWelcomeToSelectPhone extends TimeFuncs
  with Serializable
  with LogUtil
  with IopFuncs{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 120000L
  private val SOURCE_ID = "4"  //活动类型：2【换机事件】 3【开关机事件】 4【位置事件】 5【访问APP事件】
  val classNameStr = "SendWelcomeToSelectPhone"

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

    //    本程序只计算sourceId为"4"的事件
    // 从hbase获取活动的相关信息 活动ID，（Array(活动允许的基站列表)，短信内容，是否为组合事件的标记：默认为false）
    val lacCiActIdListBro = BroadcastWrapper[mutable.HashMap[String, (Set[String], String, Boolean)]](ssc
      , getActInfo(SOURCE_ID).map(actInfo =>
        (actInfo._1, (actInfo._2._1.toSet, actInfo._2._2, actInfo._2._3))
      ))

    def updateBroadcast() {
      //每隔1分钟,每天8点后，每天18点前，更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        lacCiActIdListBro.update(getActInfo(SOURCE_ID)
          .map(actInfo => (actInfo._1, (actInfo._2._1.toSet, actInfo._2._2, actInfo._2._3)))
          , blocking = true)
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
      if (f.length >= 25 && f(7).nonEmpty) {
        true
      } else{
        false
      }
    }).map(m => {
      //(phone_no, ((start_time  ,start_time_long) ,lac   ,cell  ,phone_no))
      (  m(7),     ((m(11)       ,m(9).toLong)     ,m(19) ,m(20) ,m(7) ))
    }).foreachRDD(rdd =>{
      // 流处理时间在8点到18点之间
      if (getIopSalesTimeFlag) {
        updateBroadcast
        if (lacCiActIdListBro.value.size > 0) {
          rdd.partitionBy(new HashPartitioner(100)).foreachPartition(partition =>{
            val targetTopic = "YZ_TD_IOP_SELECT_PHONE"
            val groupActTmpTopic = "YZ_TD_IOP_GROUP_ACT_TMP"
            val conn = hbaseUtil.value.createHbaseConnection

            var partitionPhoneNos: Set[String] = Set()
            val userData = new ListBuffer[(String, ((String, Long), String, String, String))]

            // 本批次手机号
            partition.foreach(p =>{
              val userCurrentInfo: ((String, Long), String, String, String) = p._2
              val phoneNo = p._1

              val partitionPhoneNo = getPartitionPhoneNo(phoneNo)

              partitionPhoneNos = partitionPhoneNos.+(partitionPhoneNo)
              userData.add((partitionPhoneNo, userCurrentInfo))
            })

            //          对用户记录流程产生时间排序
            val sortedUserData = userData.sortBy(_._2._1._2)
            //          本批次手机号批量在hbase中的查找情况(排除已经营销过的用户信息)
            val userJoindActList: mutable.HashMap[String, List[String]] =
              getUsersActListByGroupFilter(hbaseUtil.value, conn, "b_yz_app_td_hbase:SelectPhoneList4", partitionPhoneNos)
            //          本批次已经营销用户
            val alreadySaledUser: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
            //          活动与基站码表
            val lacCiActIdMap = lacCiActIdListBro.value
            //            组合事件列表
            var evtEffectPutList: List[(String, String, String, String)] = List()
            //          上批次校园需求用户驻留情况
            val schoolReults: mutable.HashMap[String, (String, Long, Long)] =
              getSchoolUserStatus(hbaseUtil.value, conn, "b_yz_app_td_hbase:IOPSchoolUser", partitionPhoneNos)

            sortedUserData.foreach((line: (String, ((String, Long), String, String, String))) =>{
              val partitionPhoneNo = line._1 //13位
              // 如果营销客群中包含此人 且 在本批次内未被营销
              if (userJoindActList.contains(partitionPhoneNo)) {
                // hbase表中用户参加的活动列表
                val userActivityIdList: Set[String] = userJoindActList(partitionPhoneNo).toSet
                val userCurrentInfo = line._2
                val userCurrentLac = userCurrentInfo._2
                val userCurrentCell = userCurrentInfo._3
                val userCurrentStartTimeLong = userCurrentInfo._1._2
                val userPhoneNo = userCurrentInfo._4
                val userCurrentLacCell = userCurrentLac + "-" + userCurrentCell

                lacCiActIdMap.foreach((actInfo: (String, (Set[String], String, Boolean))) =>{
                  val actId = actInfo._1
                  val lacCellList: Set[String] = actInfo._2._1
                  val message = actInfo._2._2
                  val groupActFlag = actInfo._2._3
                  // 如果当前活动是校园活动
                  if (actId.equals("40FA") || actId.equals("40FC")) {
                    // 如果用户没有被营销
                    if (!alreadySaledUser.contains(partitionPhoneNo)) {
                      // 如果校园驻留表包含此用户
                      if (schoolReults.contains(partitionPhoneNo)) {
                        val lastStatus: (String, Long, Long) = schoolReults(partitionPhoneNo)
                        // 用户当前参加的活动
                        val lastActId = lastStatus._1
                        // 如果当前活动是此用户当前参加的活动
                        if (lastActId.equals(actId)) {
                          // 且用户当前还在校园内
                          if (lacCellList.contains(userCurrentLacCell)) {
                            // 用户上批次驻留时间(秒)
                            val lastDuration = lastStatus._3
                            // 用户上批次驻留开始时间(秒)
                            val lastStartTime = lastStatus._2
                            // startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
                            val newDuration = userCurrentStartTimeLong / 1000 - lastStartTime + lastDuration
                            // 当此用户驻留时间超过1个小时
                            if (newDuration >= 3600L) {
                              // 如果此用户当前在校园的驻留时间超过了1小时，发送信息
                              kafkaProducer.value.send(targetTopic, userPhoneNo + " " + message)
                              alreadySaledUser.update(partitionPhoneNo, actId)
                            }
                            schoolReults.update(partitionPhoneNo, (actId, userCurrentStartTimeLong / 1000, newDuration))
                          }
                          // 如果当前用户不在校园内
                          else {
                            schoolReults.update(partitionPhoneNo, ("X", userCurrentStartTimeLong / 1000, 0))
                          }
                        }
                      }
                    }
                    // 如果校园驻留表不包含此用户
                    else {
                      // 用户首次进入校园基站列表
                      if (
                      // 此校园活动的基站包含用户当前基站
                        lacCellList.contains(userCurrentLacCell)
                          // 且用户参加此活动
                          && userActivityIdList.contains(actId)
                      ) {
                        schoolReults.update(partitionPhoneNo, (actId, userCurrentStartTimeLong / 1000, 0))
                      }
                    }
                    // --------------------------------------------
                  }
                  else{
                    // 如果用户此时在营销基站上 且 如果此用户参加此活动
                    if (userActivityIdList.contains(actId)
                      && lacCellList.contains(userCurrentLac + "-" + userCurrentCell)
                    ) {
                      // 如果这个活动是组合事件
                      if (groupActFlag) {
                        val tmpMessage = userPhoneNo + "," + actId + "," + SOURCE_ID + "," + userCurrentStartTimeLong
                        kafkaProducer.value.send(groupActTmpTopic, tmpMessage)
                        evtEffectPutList = evtEffectPutList :+ (partitionPhoneNo, actId, SOURCE_ID, userCurrentStartTimeLong.toString)
                      }
                      // 否则如果用户在上批次和本批次未被营销过
                      else if (!alreadySaledUser.contains(partitionPhoneNo)) {
                        val sendMess = userPhoneNo + " " + message
                        // 更新已经发送用户到临时表
                        alreadySaledUser.update(partitionPhoneNo, actId)
                        kafkaProducer.value.send(targetTopic, sendMess)
                      }
                    }
                  }
                })
              }
            })

            // 更新hbase校园用户驻留表
            hbaseUtil.value.putResultByKeyList_SCHOOL(conn, "b_yz_app_td_hbase:IOPSchoolUser", schoolReults.filter(!_._2._2.equals("X")).toList)
            // 更新已经营销用户表
            hbaseUtil.value.putResultByKeyList_IOP(conn, "b_yz_app_td_hbase:SelectPhoneList4", alreadySaledUser.toList)
            // 删除已经离开校园的用户
            hbaseUtil.value.deleteRows(conn,"b_yz_app_td_hbase:IOPSchoolUser", schoolReults.filter(_._2._1.equals("X")).map(_._1).toList)
            // 记录组合事件中单个事件成立到HBASE
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
