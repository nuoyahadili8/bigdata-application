package com.teradata.bigdata.iop

import java.util.{Calendar, Date, Properties}

import com.teradata.bigdata.iop.utils.IopFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.{LogUtil, TimeFuncs}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
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
  * @Description: 换机事件
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object SendWelcomeToChangePhone extends TimeFuncs
  with Serializable
  with LogUtil
  with IopFuncs{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 120000L
  private val SOURCE_ID = "2" //活动类型：2【换机事件】 3【开关机事件】 4【位置事件】 5【访问APP事件】
  val classNameStr = "SendWelcomeToChangePhone"

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
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val hbaseUtil = ssc.sparkContext.broadcast(new HbaseUtil)

    // 从hbase获取活动的相关信息 活动ID，（Array(活动运行时间列表)，短信内容，是否为组合事件的标记：默认为false）
    val lacCiActIdListBro = BroadcastWrapper[mutable.HashMap[String, (Array[String], String, Boolean)]](ssc, getActInfo(SOURCE_ID))

    def updateBroadcast() {
      //每隔1分钟,每天8点后，每天18点前，更新广播变量
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
      if (f.length >= 25 && f(7).nonEmpty) {
        true
      } else{
        false
      }
    }).map(m => {
      //(phone_no, ((start_time, start_time_long, start_time_date), imei, phone_no))
      (m(7),((m(11),m(9).toLong,strToDate(m(10), "yyyy-MM-dd HH:mm:ss")),m(6),m(7)))
    }).foreachRDD(rdd =>{
      updateBroadcast
      rdd.partitionBy(new HashPartitioner(100)).foreachPartition(partition =>{
        // 从hbase获取活动的相关信息 活动ID，（Array(活动运行时间列表)，短信内容，是否为组合事件的标记：默认为false）
        val lacCiActIdList = lacCiActIdListBro.value

        val targetTopic = "YZ_TD_IOP_SELECT_PHONE"
        val groupActTmpTopic = "YZ_TD_IOP_GROUP_ACT_TMP"

        val conn = hbaseUtil.value.createHbaseConnection

        val partitionPhoneNos = scala.collection.mutable.Set[String]()
        val userData = new ListBuffer[(String, ((String, Long, Date), String, String))]

        // 本批次手机号
        partition.foreach(p => {
          val userCurrentInfo: ((String, Long, Date), String, String) = p._2

          val phoneNo = p._1
          val partitionPhoneNo = getPartitionPhoneNo(phoneNo)
          partitionPhoneNos += partitionPhoneNo
          userData.add((partitionPhoneNo, userCurrentInfo))
        })

        // 对用户记录流程产生时间排序,剔除imei为空字符串的情况
        val sortedUserData = userData.sortBy(_._2._1._2).filter(!_._2._2.equals(""))
        // 本批次手机号批量在hbase中的查找情况(排除已经营销过的用户信息)  手机号，List(（活动ID）)
        val userJoindActList: mutable.HashMap[String, List[String]] =
          getUsersActListByGroupFilter(hbaseUtil.value, conn, "b_yz_app_td_hbase:SelectPhoneList2", partitionPhoneNos.toSet)
        // 历史用户imei信息
        val userPhoneImeiMap: mutable.HashMap[String, String] = getUsersImei(hbaseUtil.value, conn, "b_yz_app_td_hbase:UserLastStay", partitionPhoneNos.toSet)
        // 新增用户与换机用户imei信息
        val newOrChangeUserPhoneImeiMap: mutable.HashMap[String, String] = new mutable.HashMap
        // 组合事件列表
        var evtEffectPutList: List[(String, String, String, String)] = List()
        // 本批次已经营销用户
        val alreadySaledUser: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
        sortedUserData.foreach((message: (String, ((String, Long, Date), String, String))) => {
          val partitionPhoneNo = message._1
          // 用户本批次imei信息
          val imei = message._2._2

          val userPhoneNo = message._2._3

          // 如果hbase中存有用户imei信息，参与换机运算，如果不存在，
          if (userPhoneImeiMap.contains(partitionPhoneNo)) {
            // 用户上次的imei信息
            val lastImei = userPhoneImeiMap(partitionPhoneNo)
            // 如果本批次imei与上次用户imei信息不同，说明用户发生了换机行为
            if (!lastImei.equals(imei)) {
              // 如果用户已经在本批次被营销，不参与运算，只记录imei变化
              val info: ((String, Long, Date), String, String) = message._2
              // 本条信令业务发生时间
              val currentStartTime = info._1._3
              // 如果用户与活动列表对应关系中包含此用户信息,且当前时间是在早8点到18点之间的
              if (userJoindActList.contains(partitionPhoneNo) && getIopSalesTimeFlag) {
                // 取出用户参加的活动列表
                val actList: Seq[String] = userJoindActList(partitionPhoneNo)
                actList.foreach(actId => {
                  // 如果当前生效活动包含此用户参加的活动
                  if (lacCiActIdList.contains(actId)) {
                    // （Array(活动运行时间列表)，短信内容，是否为组合事件的标记：默认为false）
                    val actInfo: (Array[String], String, Boolean) = lacCiActIdList(actId)
                    val timeSet = actInfo._1.toSet    //活动时间列表
                    val message = actInfo._2          //短信内容
                    val groupActFlag = actInfo._3     //是否组合模式
                    timeSet.foreach(StartEndTimeString => {
                      val startTime = getCurrentTimeByHour(StartEndTimeString.split("-")(0))
                      val endTime = getCurrentTimeByHour(StartEndTimeString.split("-")(1))

                      // 判断本条信令业务发生时间是否在时间范围内
                      if (currentStartTime.after(startTime) && currentStartTime.before(endTime)) {
                        //                        如果为组合事件,将数据发送至临时topic
                        if (groupActFlag) {
                          val tmpMessage = userPhoneNo + "," + actId + "," + SOURCE_ID + "," + currentStartTime
                          // 组合事件 发送到组合事件topic
                          kafkaProducer.value.send(groupActTmpTopic, tmpMessage)
                          // 组合事件列表
                          evtEffectPutList = evtEffectPutList :+ (partitionPhoneNo, actId, SOURCE_ID, currentStartTime.toString)
                        }
                        else if (!alreadySaledUser.contains(partitionPhoneNo)) {  // 非本批次已经营销用户
                          // 如果为单一事件，将数据发送至结果topic
                          alreadySaledUser.update(partitionPhoneNo, actId)
                          kafkaProducer.value.send(targetTopic, userPhoneNo + "  " + message)
                        }
                      }
                    })
                  }
                })
              }
              //            如果发生了换机，将用户imei信息存入临时表，最后存入hbase
              userPhoneImeiMap.update(partitionPhoneNo, imei)
              newOrChangeUserPhoneImeiMap.update(partitionPhoneNo, imei)
            }
            //                新增用户
          } else {
            //            如果没有从hbase找到，将用户imei信息存入临时表，最后存入hbase
            userPhoneImeiMap.update(partitionPhoneNo, imei)
            newOrChangeUserPhoneImeiMap.update(partitionPhoneNo, imei)
          }
        })

        // 新增用户与换机用户imei信息
        if (newOrChangeUserPhoneImeiMap.size > 0) putUsersImei(hbaseUtil.value, conn, newOrChangeUserPhoneImeiMap)

        // 本批次已经营销用户
        if (alreadySaledUser.size > 0) hbaseUtil.value.putResultByKeyList_IOP(conn, "b_yz_app_td_hbase:SelectPhoneList2", alreadySaledUser.toList)

        // 组合事件列表
        if (evtEffectPutList.size > 0) putGroupActUserCache(hbaseUtil.value, conn, SOURCE_ID, evtEffectPutList)
        if (conn != null) conn.close()
      })
    })
  }

}
