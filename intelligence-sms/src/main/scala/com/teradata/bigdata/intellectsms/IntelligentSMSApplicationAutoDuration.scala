package com.teradata.bigdata.intellectsms

import java.util.{Calendar, Properties}

import com.teradata.bigdata.intellectsms.users.YunmasActInfo
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.control.Breaks.{breakable, _}

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/20/020 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object IntelligentSMSApplicationAutoDuration extends TimeFuncs with Serializable with YunmasFunc{

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L
  val classNameStr = "IntelligentSMSApplicationAutoDuration"
  val log: Logger = org.apache.log4j.LogManager.getLogger(classNameStr)

  def main(args: Array[String]): Unit = {

    val kafkaProperties = new KafkaProperties
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(30))
    val hbaseUtil = new HbaseUtil
    val hbaseUtilBroadcast = ssc.sparkContext.broadcast(hbaseUtil)

    val sourceTopic = Array(kafkaProperties.integrationTopic)

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

    // 从gbase获取活动配置 pview.vw_cloudmas_rule_to_td  并广播
    val yunMasActsBroadcast: BroadcastWrapper[mutable.HashMap[String, YunmasActInfo]] = BroadcastWrapper[mutable.HashMap[String, YunmasActInfo]](ssc, getYunmasActs)

    def updateBroadcast() {
      //每隔5分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        // 更新广播变量
        yunMasActsBroadcast.update(getYunmasActs, blocking = true)
        lastTime = toDate
      }
    }

    val kafkaStreams = KafkaUtils.createDirectStream[String, String](
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](sourceTopic, kafkaParams))

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val stream = kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(7).nonEmpty) {
        true
      } else{
        false
      }
    }).map(m => {
      //     (业务流程开始时间)    ,手机号 ,所在地市  ,用户漫游类型 ,归属省  ,归属地市  ,lac   ,cell
      (m(7),((m(11),m(9).toLong) ,m(7)  ,m(1)     ,m(4)        ,m(2)   ,m(3)     ,m(19) ,m(20)))
    })

    stream.foreachRDD(rdd =>{
      updateBroadcast
      //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.partitionBy(new HashPartitioner(partitions = 200)).foreachPartition(partition =>{
        //  gbase获取活动配置 pview.vw_cloudmas_rule_to_td
        //    规则ID，（活动要求所在的城市,活动要求所在的基站,漫游类型,驻留时长）
        val yunmasActs: mutable.Map[String, YunmasActInfo] = yunMasActsBroadcast.value

        //hbase
        val hbaseUtilBroadcastExecutor = hbaseUtilBroadcast.value
        val hbaseConnection: Connection = hbaseUtilBroadcastExecutor.createHbaseConnection
        val targetTopic = "YZ_TD_YUNMAS_NEW"

        var partitionPhoneNos: Set[String] = Set()
        var userData: List[(String, ((String, Long), String, String, String, String, String, String, String))] = List()

        partition.foreach(p =>{
          val userCurrentInfo = p._2
          val phoneNo = p._1
          //返回结果：手机号最后2位+手机号
          val partitionPhoneNo = getPartitionPhoneNo(phoneNo)
          partitionPhoneNos = partitionPhoneNos.+(phoneNo)

          userData = (partitionPhoneNo,userCurrentInfo) :: userData
        })

        //从hbase取出用户的最后状态：b_yz_app_td_hbase:TourMasUserNew
        val yunmasUserLastStatus = getYunmasUserLastStatusTest(hbaseUtilBroadcastExecutor, hbaseConnection, partitionPhoneNos.toList)

        userData
          .sortBy(_._2._1._2)  //按进入这个需求的时间排序
          .foreach(kLine =>{
          val userCurrentInfo = kLine._2
          breakable{
            // gbase获取活动配置 pview.vw_cloudmas_rule_to_td
            yunmasActs.filter(act => act._2.stayDuration>0).foreach(act => {
              val actId = act._1   // rule_id
              val yunmasActInfo = act._2   //活动要求用户当前所在城市、活动要求用户当前所在基站列表范围、要求的漫游类型、要求的驻留时长(秒)

              //判断条件并发送消息
              val isContinue = judgeConditionsAndSendDuration(kafkaProducer.value
                , targetTopic
                , userCurrentInfo
                , actId
                , yunmasActInfo
                , yunmasUserLastStatus
              )
              if (!isContinue){
                break
              }
            })
          }
        })

        // 1.更新用户在hbase的驻留时长状态
        // 2.删除已经离开需求区域的用户
        updateAndDeleteUserStatusDuration(hbaseUtilBroadcastExecutor, hbaseConnection, yunmasUserLastStatus)

        if (hbaseConnection != null) hbaseConnection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
