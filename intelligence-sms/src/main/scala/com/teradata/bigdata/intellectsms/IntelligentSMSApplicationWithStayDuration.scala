package com.teradata.bigdata.intellectsms

import java.util.{Calendar, Properties}

import com.teradata.bigdata.intellectsms.area._
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.SparkConfig
import com.teradata.bigdata.util.tools.TimeFuncs
import com.xiaoleilu.hutool.util.StrUtil
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Project:
  * @Description: 智能短信  与Hbase交互 过滤驻留时长，符合条件的记录放入kafka
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object IntelligentSMSApplicationWithStayDuration extends TimeFuncs with Serializable {

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L
  val classNameStr = "IntelligentSMSApplicationWithStayDuration"

  def main(args: Array[String]): Unit = {
    val kafkaProperties = new KafkaProperties()
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(120))

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

    val kafkaStreams = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](sourceTopic, kafkaParams))

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

    // 获取公安的用户列表（监控人群，全是手机号）甘肃、宁夏号段
    val monitorPersionBroadcast: Broadcast[Set[String]] =
      ssc.sparkContext.broadcast(ssc.sparkContext.textFile("hdfs://nmsq/user/b_yz_app_td/gansu_ningxia_haoduan_file").collect().toSet)

    kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(7).length >= 11) {
        true
      } else{
        false
      }
    }).map(m => {
      //     (业务流程开始时间)    ,手机号 ,所在地市  ,用户漫游类型 ,归属省  ,归属地市  ,lac   ,cell
      (m(7),((m(11),m(9).toLong) ,m(7)  ,m(1)     ,m(4)        ,m(2)   ,m(3)     ,m(19) ,m(20)))
    }).foreachRDD(rdd =>{
      rdd.partitionBy(new HashPartitioner(200)).foreachPartition(partition =>{
        val targetTopic = "YZ_TD_YUNMAS"

        val area = new AreaList
        val eerduosiArea = new EerduosiAreaList
        val wulanhaoteAreaList = new WulanhaoteAreaList
        val wengniuteAreaList = new WengniuteAreaList
        val minhangjichangAreaList = new MinhangjichangAreaList
        val naimanqiAreaList = new NaimanqiAreaList
        val chifengsongshanlaolin = new Chifengsongshanlaolin
        //        val bayannaoerAreaList = new BayannaoerAreaList

        val hbaseUtilValue = hbaseUtilBroadcast.value
        val conn: Connection = hbaseUtilValue.createHbaseConnection
        val monitorPersion = monitorPersionBroadcast.value

        val badanjilin = area.badanjilin
        val elunchun = area.elunchun

        val honghuaerji = area.honghuaerji
        val wulanbuhe = area.wulanbuhe
        //        val tuoxian = area.tuoxian
        val eerduosi = eerduosiArea.eerduosi
        val wulanhaote = wulanhaoteAreaList.wulanhaote
        val wengniute = wengniuteAreaList.wengniute
        val jichang = minhangjichangAreaList.jichang
        val naimanqi = naimanqiAreaList.naimanqi
        val songshanguoyinglaofulinchang = chifengsongshanlaolin.songshanguoyinglaofulinchang
        val songshanguoyingdanianzilinchang = chifengsongshanlaolin.songshanguoyingdanianzilinchang
        //        val bayannaoerlvye = bayannaoerAreaList.bayannaoerlvye

        // 时间排序，以便数据落到同一计算单元
        val sortedPartition = partition.toList.sortBy(_._2._1._2)

        val distinctPartition: List[String] = sortedPartition.map(_._1).distinct

        //b_yz_app_td_hbase:TourMasUserNew 智能短信用户驻留时长实时表
        val lastUserStatus: mutable.Map[String, (String, Long, Long)] = hbaseUtilValue.getResultByKeyList_MAS(conn, "b_yz_app_td_hbase:TourMasUser", distinctPartition)

        //        内蒙领导离开状态查询结果
        val leadersStatus: mutable.HashMap[String, (String, String)] = hbaseUtilValue.getResultByKeyList_Leader("b_yz_app_td_hbase:TourMasLeaderUser", distinctPartition)
        val chifengLeadersStatus = leadersStatus.filter(_._2._1.equals("134"))
        val eerduosiLeadersStatus = leadersStatus.filter(_._2._1.equals("47"))
        val huhehaoteLeadersStatus = leadersStatus.filter(_._2._1.equals("141"))

        //        状态变化表
        val putLeadersStatusMap: mutable.HashMap[String, (String, String)] = new scala.collection.mutable.HashMap

        sortedPartition.foreach(kLine =>{
          val line: ((String, Long), String, String, String, String, String, String, String) = kLine._2

          val phone_no = line._2
          val local_city = line._3
          val roam_type = line._4
          val owner_province = line._5
          val owner_city = line._6
          val lac = line._7
          val ci = line._8
          val lac_ci = lac + "-" + ci
          val startTime: String = line._1._1
          val startTimeLong: Long = line._1._2

          val stringLine = startTime + "|" +
            phone_no + "|" +
            local_city + "|" +
            roam_type + "|" +
            owner_province + "|" +
            owner_city + "|" +
            lac + "|" +
            ci

          // 判断是否漫入赤峰的人
          val chifengFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0476") && !owner_city.equals("0476")) {
              true
            } else{
              false
            }
          }

          // 如果用户是漫入到鄂伦春的
          val elunchunFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0470") && !owner_city.equals("0470") && elunchun.contains(lac_ci)){
              true
            } else{
              false
            }
          }

          val tongliaoFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0475") && !owner_city.equals("0475")) true else false
          }

          val badanjilinFunc = {
            var successFlag = false
            var phone_no_head = ""
            try {
              if (phone_no.startsWith("1064")) {
                phone_no_head = phone_no.substring(0, 9)
              }
              else {
                phone_no_head = phone_no.substring(0, 7)
              }

              if ((roam_type.equals("1") || roam_type.equals("2")) && badanjilin.contains(lac_ci) && !monitorPersion.contains(phone_no_head)) {
                successFlag = true
              } else {
                successFlag = false
              }
            }
            catch {
              case e => println("badanjilinFunc,error phoneno is:" + phone_no)
            }
            successFlag
          }

          val eerduosiFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0477") && !owner_city.equals("0477")) {
              true
            } else {
              false
            }
          }

          val honghuaerjiFunc = {
            if (honghuaerji.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          val wulanbuheFunc = {
            if (wulanbuhe.contains(lac_ci) && !roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0483") && !owner_city.equals("0483")) {
              true
            } else {
              false
            }
          }

          /*val tuoxianFunc = {
            if (tuoxian.contains(lac_ci)
              && !roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0471")
              && !owner_city.equals("0471")
            ) true else false
          }*/
          val huhehaoteFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0471") && !owner_city.equals("0471")
            ) true else false
          }
          val wulanhaoteFunc = {
            if (roam_type.equals("2") && local_city.equals("0482") && wulanhaote.contains(lac_ci)) {
              true
            } else {
              false
            }
          }
          val eerduosiLeadersFunc = {
            if (local_city.equals("0477") && eerduosi.contains(lac_ci)) {
              true
            } else {
              false
            }
          }
          val chifengLeaderFunc = {
            if (local_city.equals("0476")) {
              true
            } else {
              false
            }
          }
          val huhehaoteLeaderFunc = {
            if (local_city.equals("0471")) {
              true
            } else {
              false
            }
          }
          val wengniuteFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0476") && wengniute.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          val minhangjichangFunc = {
            if (jichang.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          val naimanqiFunc = {
            if (naimanqi.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          val zgnaimanqizhengfaweiFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && naimanqi.contains(lac_ci) && !owner_city.equals("0475")) {
              true
            } else {
              false
            }
          }

          val wuhaiFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0473") && !owner_city.equals("0473")) {
              true
            } else {
              false
            }
          }

          // 赤峰市松山区国营老府林场 150
          val songshanguoyinglaofulinchangFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0476") && songshanguoyinglaofulinchang.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          // 赤峰市松山区国营大碾子林场 153
          val songshanguoyingdanianzilinchangFunc = {
            if (!roam_type.equals("4") && !roam_type.equals("") && local_city.equals("0476") && songshanguoyingdanianzilinchang.contains(lac_ci)) {
              true
            } else {
              false
            }
          }

          //          val bayannaoerlvyeFunc ={
          //            if (!roam_type.equals("4")
          //              && !roam_type.equals("")
          //              && bayannaoerlvye.contains(lac_ci)
          //              && !owner_city.equals("0478") //区号
          //            ) true else false
          //          }

          // 包含驻留时长的 手机号，（需求编号，进入需求时间，时长）
          if (lastUserStatus.contains(phone_no)) {
            val lastStatus: (String, Long, Long) = lastUserStatus(phone_no)
            val lastEventType = lastStatus._1

            // 驻留时长判断及发送函数
            def judgeUserStayDuration(areaFunc: Boolean, timeout: Long): Unit ={
              if (areaFunc) {
                // 用户上批次驻留时间(秒)
                val lastDuration = lastStatus._3
                // 用户上批次驻留开始时间(秒)
                val lastStartTime = lastStatus._2
                // startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
                val newDuration = startTimeLong / 1000 - lastStartTime + lastDuration
                // 当此用户驻留时间超过1个小时
                if (newDuration >= timeout) {
                  kafkaProducer.value.send(targetTopic, lastEventType + "|" + stringLine)
                }
                lastUserStatus.update(phone_no, (lastEventType, startTimeLong / 1000, newDuration))
              } else{
                lastUserStatus.update(phone_no, ("X", startTimeLong / 1000, 0))
              }
            }

            // 漫入赤峰人群(所在地市，漫游类型)id:2
            // 赤峰2
            // if (lastEventType.equals("2")) judgeUserStayDuration(chifengFunc, 3600L)
            // 鄂伦春11
            //if (lastEventType.equals("11")) judgeUserStayDuration(elunchunFunc, 600L)

            // 巴丹吉林旅游区14
            if (lastEventType.equals("14")) judgeUserStayDuration(badanjilinFunc, 60L * 60)
            // 呼伦贝尔市红花尔基林业局15
            if (lastEventType.equals("15")) judgeUserStayDuration(honghuaerjiFunc, 60L * 30)
            // 阿拉善,乌兰布和23
            if (lastEventType.equals("23")) judgeUserStayDuration(wulanbuheFunc, 60L * 60)
            // 兴安盟,乌兰浩特33
            if (lastEventType.equals("33")) judgeUserStayDuration(wulanhaoteFunc, 60L * 60 * 3)
            // 赤峰，中共翁牛特旗委员会宣传部22
            if (lastEventType.equals("22")) judgeUserStayDuration(wengniuteFunc, 60L * 60 * 5)
            // 通辽市旅游发展委员会9
            if (lastEventType.equals("9")) judgeUserStayDuration(tongliaoFunc, 60L * 4)
            // 民航机场55
            if (lastEventType.equals("55")) judgeUserStayDuration(minhangjichangFunc, 60L * 10)
            // 通辽，奈曼旗82
            if (lastEventType.equals("82")) judgeUserStayDuration(naimanqiFunc, 60L * 5)
            // 通辽，奈曼旗88
            if (lastEventType.equals("88")) judgeUserStayDuration(zgnaimanqizhengfaweiFunc, 60L * 30)
            // 赤峰2
            if (lastEventType.equals("2")) judgeUserStayDuration(chifengFunc, 60L * 30)
            // 乌海文体旅游广电局126
            if (lastEventType.equals("126")) judgeUserStayDuration(wuhaiFunc, 60L * 3)
            // 乌海文体旅游广电局126
            // if (lastEventType.equals("126")) judgeUserStayDuration(wuhaiFunc, 60L * 3)
            // 托县25
            // else if (lastEventType.equals("25")) {
            //  judgeUserStayDuration(tuoxianFunc, 600L)
            //}
            // 呼和浩特27
            // else if (lastEventType.equals("27")) {
            //  judgeUserStayDuration(huhehaoteFunc, 60L)
            // }

            // 漫入    内蒙古巴彦绿业实业有限公司143
            // if (lastEventType.equals("143")) judgeUserStayDuration(bayannaoerlvyeFunc, 60L * 30)

            // 赤峰市松山区国营老府林场150
            if (lastEventType.equals("150")) judgeUserStayDuration(songshanguoyinglaofulinchangFunc, 60L * 30)
            // 赤峰市松山区国营大碾子林场 153
            if (lastEventType.equals("153")) judgeUserStayDuration(songshanguoyingdanianzilinchangFunc, 60L * 30)
          }
          // 不包含驻留时长的
          else{
            def updateStatus(eventType: String): Unit = {
              lastUserStatus.update(phone_no, (eventType, startTimeLong / 1000, 0))
            }

            //                漫入赤峰，更新临时列表
            //            if (chifengFunc) updateStatus("2")
            //            鄂伦春11
            //            if (elunchunFunc) updateStatus("11")

            //            巴丹吉林旅游区14
            if (badanjilinFunc) updateStatus("14")
            //              红花尔基15
            if (honghuaerjiFunc) updateStatus("15")
            //            阿拉善乌兰布和23
            if (wulanbuheFunc) updateStatus("23")
            //            兴安盟乌兰浩特33
            if (wulanhaoteFunc) updateStatus("33")
            //            赤峰，翁牛特旗22
            if (wengniuteFunc) updateStatus("22")
            //              通辽市旅游发展委员会9
            if (tongliaoFunc) updateStatus("9")
            //            明航机场55
            if (minhangjichangFunc) updateStatus("55")
            //            通辽，奈曼旗82
            if (naimanqiFunc) updateStatus("82")
            // 中共奈曼旗委政法委员会 88
            if (zgnaimanqizhengfaweiFunc) updateStatus("88")
            //            赤峰2
            if (chifengFunc) updateStatus("2")
            //            乌海文体旅游广电局126
            if (wuhaiFunc) updateStatus("126")
            //            乌海文体旅游广电局126
            //            if (wuhaiFunc) updateStatus("126")
            //            else if (eerduosiFunc) {
            //              lastUserStatus.update(phone_no, ("7", startTimeLong / 1000, 0))
            //            }
            //            else if (tuoxianFunc) {
            //              lastUserStatus.update(phone_no, ("25", startTimeLong / 1000, 0))
            //            }
            //            if (huhehaoteFunc) {
            //              lastUserStatus.update(phone_no, ("27", startTimeLong / 1000, 0))
            //            }
            //            if(bayannaoerlvyeFunc) updateStatus("143")

            // 赤峰市松山区国营老府林场 150
            if (songshanguoyinglaofulinchangFunc) updateStatus("150")
            // 赤峰市松山区国营大碾子林场 153
            if (songshanguoyingdanianzilinchangFunc) updateStatus("153")
          }

          // 1.判断领导是否离开去了外地
          // 2.判断领导是否从外地返回，并发出信息
          def judgeLeaderStatus(leaderStatus: mutable.HashMap[String, (String, String)]
                                , eventType: String   // 需求ID
                                , eventCity: String  // 要求应该在的地市
                                , judgeFunc: Boolean // 判断函数
                               ): Unit = {
            if (leaderStatus.contains(phone_no)) {
              val leaveFlag = leaderStatus(phone_no)._2
              // 鄂尔多斯领导离开了本地，状态初始值为0，且领导不在本地时，将状态置为离开状态1
              if ("0".equals(leaveFlag) && !local_city.equals(eventCity)) {
                leaderStatus.update(phone_no, (eventType, "1"))
                putLeadersStatusMap.update(phone_no, (eventType, "1"))
              }
              // 鄂尔多斯领导回到了本地，当状态为1，且此时在本地时
              if ("1".equals(leaveFlag) && judgeFunc) {
                kafkaProducer.value.send(targetTopic, eventType + "|" + stringLine)
                leaderStatus.update(phone_no, (eventType, "0"))
                putLeadersStatusMap.update(phone_no, (eventType, "0"))
              }
            }
          }

          // 鄂尔多斯7，领导（hbase配置ID为7，发送ID为47）
          judgeLeaderStatus(eerduosiLeadersStatus, "47", "0477", eerduosiLeadersFunc)
          // 赤峰2，领导
          judgeLeaderStatus(chifengLeadersStatus, "134", "0476", chifengLeaderFunc)
          // 呼和浩特领导141
          // judgeLeaderStatus(huhehaoteLeadersStatus, "141", "0471", huhehaoteLeaderFunc)
        })
        // 将领导的状态信息更新
        hbaseUtilValue.putByKeyColumnList_Leader(conn, "b_yz_app_td_hbase:TourMasLeaderUser", putLeadersStatusMap.toList)
        // 更新新进入计时区域和驻留时长更新区域的用户
        val putResultList: List[(String, (String, Long, Long))] = lastUserStatus.filter(!_._2._1.equals("X")).toList

        hbaseUtilValue.putByKeyColumnList_MAS(conn, "b_yz_app_td_hbase:TourMasUser", putResultList)

        // 删除hbase已经离开的用户 根据手机号rowkey删除
        val delResultList = lastUserStatus.filter(_._2._1.equals("X")).map(_._1).toList
        hbaseUtilValue.deleteRows(conn,"b_yz_app_td_hbase:TourMasUser", delResultList)
        if (conn != null) conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
