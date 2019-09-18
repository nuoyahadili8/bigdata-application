package com.teradata.bigdata.iop

import java.util.{Calendar, Properties}

import com.teradata.bigdata.iop.utils.IopFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.LogUtil
import javax.script.ScriptEngineManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * 此程序计算跨事件源组合事件
  * 从kafka接收已成立单事件源（4.位置 3.开关机 2.换机 5.APP使用） IOP-GROUP-ACT-TMP
  * 根据IOP规则解析中的组合事件成立规则（3&&（2||4））
  * 从hbase中查找30min内有效的成立事件 IopEffectEventCache
  * 营销结果发送到topic:IOP-SEND-MESSAGE-SELECT-PHONE
  * 同时存储发送结果到hbase ：SelectPhoneListGroup
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object SendWelcomeToGroupUser extends IopFuncs with LogUtil{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 120000L
  val classNameStr = "SendWelcomeToGroupUser"

  def main(args: Array[String]): Unit = {
    val kafkaProperties = new KafkaProperties
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.set("spark.streaming.kafka.consumer.poll.ms", "60000").setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(30))

    val topics = Array("IOP-GROUP-ACT-TMP")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> kafkaProperties.kafkaBrokers.mkString(","),
      "group.id" -> classNameStr,
      "zookeeper.connection.timeout.ms" -> "100000"
    )
    val hbaseUtil = ssc.sparkContext.broadcast(new HbaseUtil)

    //从组合事件码表hbase b_yz_app_td_hbase:allOfEffActivityGroup 取出(活动ID，（组合事件字符串【如： 2&&3&&4】，短信内容）)
    val groupActInfoBro = BroadcastWrapper[mutable.HashMap[String, (String, String)]](ssc, getGroupActInfo)

    def updateBroadcast() {
      //每隔1分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        groupActInfoBro.update(getGroupActInfo, blocking = true)
        lastTime = toDate
      }
    }

    val kafkaStreams: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics,kafkaParams))

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", kafkaProperties.kafkaBrokers.mkString(","))
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      log.warn("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val filterStream = kafkaStreams.map(m => {
      val messArray = m.value().split(",", -1)
      val phoneNo = messArray(0)
      val actId = messArray(1)
      val sourceId = messArray(2)
      val startTimeLong = messArray(3)
      (phoneNo, (actId, sourceId, startTimeLong, phoneNo))
    })

    filterStream.foreachRDD(rdd => {

      // 在营销事件范围内 早8点到晚18点
      if (getIopSalesTimeFlag) {
        updateBroadcast
        if (groupActInfoBro.value.size > 0) {
          rdd.partitionBy(new HashPartitioner(10)).foreachPartition(partition => {
            val targetTopic = "IOP-SEND-MESSAGE-SELECT-PHONE"

            val conn = hbaseUtil.value.createHbaseConnection
            var partitionPhoneNos: Set[String] = Set()
            var userData: List[(String, (String, String, String, String))] = List()
            partition.foreach(mess => {
              val phoneNo = mess._1.toString
              val partitionPhoneNo = getPartitionPhoneNo(phoneNo)
              val userInfo = mess._2
              partitionPhoneNos = partitionPhoneNos.+(partitionPhoneNo)
              userData = userData :+ (partitionPhoneNo, userInfo)
            })
            // 组合活动信息
            val groupActInfo: mutable.HashMap[String, (String, String)] = groupActInfoBro.value
            // 本批次手机号批量在hbase中的查找已经营销过的用户（组合事件）
            val userJoindActList = getSaledGroupActUser(hbaseUtil.value, conn, partitionPhoneNos)
            // 本批次手机号批量在hbase中查找多种事件源成立的缓存情况(此处是事件为2、3、4、5营销过的客群)  phoneno,[actId, sourceId]
            val groupActUserCache: mutable.Map[String, mutable.HashMap[String, String]] = getGroupActUserCache(hbaseUtil.value, conn, partitionPhoneNos)
            // 本批次已经营销用户
            val alreadySaledUser: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
            // 将用户信息按照时间序列排序
            val sortedUSerData = userData.sortBy(_._2._3)
            // 逻辑表达式解析器
            val manager = new ScriptEngineManager
            val engine = manager.getEngineByName("js")

            sortedUSerData.foreach(mess => {
              val partitionPhoneNo = mess._1
              val (userCurrentActId, sourceId, startTimeLong, userPhoneNo) = mess._2
              // 获取缓存中还有效的事件成立信息  actId, sourceId
              val actSourceSet: mutable.HashMap[String, String] =
                groupActUserCache.getOrElse(partitionPhoneNo, new mutable.HashMap())
              // 组合事件中包含此活动 且 本批次已营销活动中不包含此用户 且 本月已营销用户不包含此用户
              if (groupActInfo.contains(userCurrentActId)
                && !alreadySaledUser.contains(partitionPhoneNo)
                && !userJoindActList.contains(partitionPhoneNo)) {
                // （组合事件字符串【如： 2&3&4】，短信内容）
                val actInfo = groupActInfo(userCurrentActId)
                val message = actInfo._2
                var evtSetEstablishedFormulas = actInfo._1

                // 根据kafka流入的事件成立信息，将事件源ID替换为true
                evtSetEstablishedFormulas = evtSetEstablishedFormulas.replaceAll(sourceId, "true")
                // 如果hbase中存有统一事件发生的事件成立情况，将事件源更新为true
                if (actSourceSet.contains(userCurrentActId)) {
                  evtSetEstablishedFormulas
                    = evtSetEstablishedFormulas.replaceAll(actSourceSet(userCurrentActId), "true")
                }
                // 其他的事件源更新为false  [将其他事件没有替换调的置成false]
                evtSetEstablishedFormulas
                  = evtSetEstablishedFormulas.replaceAll("(?m)\\d+", "false")

                // 组合事件成立的情况下
                if (engine.eval(evtSetEstablishedFormulas).toString.toBoolean) {
                  kafkaProducer.value.send(targetTopic, userPhoneNo + "  " + message)
                  alreadySaledUser.update(partitionPhoneNo, userCurrentActId)
                }
              }
            })

            hbaseUtil.value.putResultByKeyList_IOP(conn, "SelectPhoneListGroup", alreadySaledUser.toList)
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
