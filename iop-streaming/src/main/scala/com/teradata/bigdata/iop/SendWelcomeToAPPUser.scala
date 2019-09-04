package com.teradata.bigdata.iop

import java.util.{Calendar, Properties}

import com.teradata.bigdata.iop.utils.IopFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.{LogUtil, TimeFuncs}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object SendWelcomeToAPPUser extends TimeFuncs with Serializable with LogUtil with IopFuncs{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 120000L
  private val SOURCE_ID = "5"
  val classNameStr = "SendWelcomeToAPPUser"

  def main(args: Array[String]): Unit = {
    val kafkaProperties = new KafkaProperties()
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(30))

    //    监听app解析hdfs目录  http信令(东华信过来的)
    val appSourceDataFolder = "hdfs://nmdsj133nds/user/caodongwei/netmobilelog/NM_NET_LOG/"

    val unifiedStream: DStream[String] = ssc.textFileStream(appSourceDataFolder)

    val hbaseUtil = ssc.sparkContext.broadcast(new HbaseUtil)

    // 从hbase获取活动的相关信息 活动ID，（Array(APP名称,)，短信内容，是否为组合事件的标记：默认为false）
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

    val filterStream = unifiedStream.map(m => {
      m.split("\\|", -1)
    }).filter(array => {
      if(array.length==61){
        if ((array(0) != null || !"".equals(array(0))) && (array(36) != null || !"".equals(array(36)))) true else false
      }else false
    }).map(array => {
      val phoneNo = array(0)          // 手机号
      val is_noise = array(15)        // 是否噪音
      val root_domain = array(19)     // 根域名
      val w3_prod_id = array(20)      // 根域名对应产品ID
      val w3_prod_name = array(21)    // 根域名对应产品名称
      val is_w3_browser = array(24)   // 是否浏览器
      val lac = array(28)             // lac
      val ci = array(29)              // ci
      val rat_type = array(33)        // 网络类型
      val start_time = array(36)      // 开始时间
      val ua_prod_label = array(49)   // 产品标签
      val ua_label_name1 = array(50)  // 产品标签名称1
      val ua_label_name2 = array(51)  // 产品标签名称2
      (phoneNo, (is_noise
        , root_domain
        , w3_prod_id
        , w3_prod_name
        , is_w3_browser
        , lac
        , ci
        , rat_type
        , start_time
        , ua_prod_label
        , ua_label_name1
        , ua_label_name2
        , phoneNo))
    })

    filterStream.foreachRDD(rdd => {

      updateBroadcast

      if (lacCiActIdListBro.value.size > 0) {
        rdd.partitionBy(new HashPartitioner(100)).foreachPartition(partition => {
          // 从hbase获取活动的相关信息 b_yz_app_td_hbase:allOfEffActivity 营销事件配置表
          val lacCiActIdList = lacCiActIdListBro.value

          val targetTopic = "IOP-SEND-MESSAGE-SELECT-PHONE"
          val groupActTmpTopic = "IOP-GROUP-ACT-TMP"

          val conn = hbaseUtil.value.createHbaseConnection

          var partitionPhoneNos = Set[String]()
          //
          val userData = new ListBuffer[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String))]

          //          本批次手机号
          partition.foreach(p => {
            val userCurrentInfo = p._2

            val phoneNo = p._1
            if(phoneNo.length>=11){
              val partitionPhoneNo = phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
              partitionPhoneNos = partitionPhoneNos.+(partitionPhoneNo)
              userData.add((partitionPhoneNo, userCurrentInfo))
            }
          })


          //          对用户记录流程产生时间（start_time）排序  目的：微批中防止顺序乱  sortBy不改变数据结构
          val sortedUserData = userData.sortBy(_._2._9)
          //          本批次手机号批量在hbase中的查找情况(排除已经营销过的用户信息) 访问APP时间客群表(b_yz_app_td_hbase:SelectPhoneList5)
          // b_yz_app_td_hbase:SelectPhoneList5  rowkey:手机号  活动ID  发送月份
          /**
            * rowkey:手机号      活动ID         初始化时间戳           发送月份(当前时间) 默认：配置这个活动4618的上个月时间
            * 0013474827300     column=0:4618, timestamp=123456789, value=201810
            */
          val userJoindActList: mutable.HashMap[String, List[String]] =
            getUsersActList(hbaseUtil.value, conn, "b_yz_app_td_hbase:SelectPhoneList5", partitionPhoneNos)

          //          本批次已经营销用户
          val alreadySaledUser: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap

          sortedUserData.foreach(message => {
            val partitionPhoneNo = message._1

            if (!alreadySaledUser.contains(partitionPhoneNo)) {
              val info = message._2
              val currentStartTime = info._9  // 开始时间（访问时间）

              val requestAppName = info._4  // 用户已经访问的APP名称
              val phoneNo = info._13

              // userJoindActList：排除已经营销过的用户信息
              if (userJoindActList.contains(partitionPhoneNo)) {
                val actList = userJoindActList(partitionPhoneNo)

                //排除已经营销过的手机号
                for (actId <- actList if !alreadySaledUser.contains(partitionPhoneNo)) {
                  //从hbase取出的活动配置表 活动ID，（Array(APP名称,)，短信内容，是否为组合事件的标记：默认为false）
                  if (lacCiActIdList.contains(actId)) {
                    //活动ID，（APP名称列表，短信内容，是否为组合事件的标记：默认为false）
                    val actInfo: (Array[String], String, Boolean) = lacCiActIdList(actId)
                    // 用户参加活动的APP名称
                    val keyWordInfoArray = actInfo._1
                    val keyWordSet = keyWordInfoArray(0).split("-") //以-分割的多个app
                    val keyWordStartDate = keyWordInfoArray(1) //活动ID要求访问app的时间范围
                    val keyWordEndDate = keyWordInfoArray(2)
                    val keyWordStartHour = keyWordInfoArray(3)
                    val keyWordEndHour = keyWordInfoArray(4)

                    val message = actInfo._2
                    val groupActFlag = actInfo._3
                    for (keyWord <- keyWordSet if !alreadySaledUser.contains(partitionPhoneNo)) {
                      // 判断用户访问的app与活动要求的app范围内，且在活动时间范围内
                      if (requestAppName.contains(keyWord)
                        && strToDate(currentStartTime, "yyyy-MM-dd HH:mm:ss:SSS").after(getTimeByHour(keyWordStartDate, keyWordStartHour))
                        && strToDate(currentStartTime, "yyyy-MM-dd HH:mm:ss:SSS").before(getTimeByHour(keyWordEndDate, keyWordEndHour))) {
                        //                        如果为组合事件,将数据发送至临时topic
                        if (groupActFlag) {
                          kafkaProducer.value.send(groupActTmpTopic, s"$phoneNo,$actId,3,$message")
                          println(s"$phoneNo,$actId,5,$message")
                        } else {
                          //                          如果为单一事件，将数据发送至结果topic
                          alreadySaledUser.update(partitionPhoneNo, actId) // 更新到已营销客群中
                          kafkaProducer.value.send(targetTopic, phoneNo + "  " + message + " productTime:" + currentStartTime)
                          println(phoneNo + "  " + message + " productTime:" + currentStartTime)
                        }
                      }
                    }
                  }
                }
              }
            }
          })

          hbaseUtil.value.putResultByKeyList_IOP(conn, "SelectPhoneList5", alreadySaledUser.toList)

          if (conn != null) conn.close()
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
