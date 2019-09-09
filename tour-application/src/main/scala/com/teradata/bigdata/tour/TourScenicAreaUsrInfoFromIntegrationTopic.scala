package com.teradata.bigdata.tour

import java.lang._
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.JSONObject
import com.teradata.bigdata.tour.bean.ScenicInfo
import com.teradata.bigdata.tour.utils.TourFuncs
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.teradata.TeradataConnect
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * @Project:
  * @Description: 旅游应用从整合topic【YZ_APP_TD_234G_DPI_DATA】获取信息并计算
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/2/002 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object TourScenicAreaUsrInfoFromIntegrationTopic
  extends TimeFuncs
    with Serializable
    with TourFuncs
    with TeradataConnect {

  var lastSendTime = getCurrentTimeMillis

  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L
  val classNameStr = "TourScenicAreaUsrInfoFromIntegrationTopic"

  def main(args: Array[String]) {
    val kafkaProperties = new KafkaProperties
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf
      .set("spark.streaming.kafka.consumer.poll.ms", "60000")
      .setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(20))
    val topics = Array(kafkaProperties.integrationTopic)
    val brokers = kafkaProperties.kafkaBrokers.mkString(",")
    val groupId = classNameStr

    val tdConn = interface177Connect


    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val kafkaStreams: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream[String, String](
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val hbaseUtilDriver = new HbaseUtil
    val connDriver = hbaseUtilDriver.createHbaseConnection


    val jedisDmz = getDmzRedisConnect
    jedisDmz.auth("nmtour")
    val travelSceneryMap: mutable.Map[String, String] = getSceneryInfoFromDmz(jedisDmz)
    println("travelSceneryMap:" + travelSceneryMap.size)

    //    异常用户集合
    //    460022482991538,15248568038,161026,1
    val abNormalUser =
    ssc.sparkContext.textFile("hdfs://nmsq/user/b_yz_app_td/TB_TRAVAL_USER_STAYDURATION_MUL_DAY.txt")
      .map(_.split(",")(1))
      .filter(_.length >= 11)
      .map(getPartitionPhoneNo(_))
      .collect().toSet


    println("abNormalUser size:" + abNormalUser.size)
    val abNormalUserBro = ssc.sparkContext.broadcast(abNormalUser)

    // 景区的配置码表 lacCellId, scenicInfo(sceneryId, sceneryName, cityId, alarmValue)
    // 基站ID scenicInfo(景区ID，景区名称,地市,景点人数上限阈值)
    val selectLacCi = BroadcastWrapper[collection.Map[String, ScenicInfo]](ssc, getSelectLacCiAllRows(tdConn))

    def updateBroadcast(): Unit = {
      //每隔5分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {
        selectLacCi.update(getSelectLacCiAllRows(tdConn), blocking = true)
        println("selectLacCi:" + selectLacCi.value.size)
        lastTime = toDate
      }
    }

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      println("kafka producer init done!")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    kafkaStreams.map(m => {
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(7).nonEmpty) {
        true
      } else {
        false
      }
    }).map(m => {
      //手机号分区字段     业务流程开始时间  ,手机号 ,imei    ,lac     ,cell   ,上行流量  ,下行流量  ,数据类型
      (m(24), Row(m(11), m(24), m(6), m(19), m(20), "", "", m(23), m(9).toLong))
    })
      .foreachRDD(rdd => {
        updateBroadcast
        val userCheckAllItr: RDD[((String, ScenicInfo), (Int, Int, Int))] = rdd
          .partitionBy(new HashPartitioner(300))
          .mapPartitions((partition: Iterator[(String, Row)]) => {
            val hbaseUtilExecutor: HbaseUtil = new HbaseUtil
            val hbaseUtilConnExecutor = hbaseUtilExecutor.createHbaseConnection
            //异常用户
            val abnormalUser: Set[String] = abNormalUserBro.value
            val TARGET_TOPIC = "YZ_TD_NM_TOUR"
            val partitionRow: List[(String, Row)] = partition.toList
              .filter(line => !abnormalUser.contains(line._1)) //排除异常用户
              // 根据start_time排序，防止顺序乱
              .sortBy(line => line._2(0).toString)

            val userLastStayList = partitionRow
              .groupBy(_._1) // 以手机号进行分组 Map[String, List[(String, Row)]]
              .mapValues(_.maxBy(_._2(0).toString)) // 取出单个用户最大时间的那条记录 Map[String, (String, Row)]
              .map(line => {
              val row = line._2._2
              val phone_no = row(1).toString
              val lac = row(3).toString
              val cell = row(4).toString
              (phone_no, lac + "-" + cell)
            }).toList

            // 用户此刻所在基站信息（剔重）
            val distinctRow = userLastStayList.map(_._1).toSet

            // 景区码表  基站ID，scenicInfo(景区ID，景区名称,地市,景点人数上限阈值)
            val valueOfSelectLacCi: collection.Map[String, ScenicInfo] = selectLacCi.value

            // 景区ID，scenicInfo(景区ID，景区名称,地市,景点人数上限阈值)
            val scenicOfSelectLacCi: collection.Map[String, ScenicInfo] = valueOfSelectLacCi.map(kv => {
              val sceneryId = kv._2.sceneryId
              (sceneryId, kv._2)
            })

            // 本批次迁入表(景区,迁入数量)
            val userCheckIn: mutable.HashMap[(String, ScenicInfo), Int] = new scala.collection.mutable.HashMap
            // 本批次迁出表(景区,迁出数量)
            val userCheckOut: mutable.HashMap[(String, ScenicInfo), Int] = new scala.collection.mutable.HashMap
            // 景区本批次 总数
            val userCheckAll: mutable.HashMap[(String, ScenicInfo), (Int, Int, Int)] = new scala.collection.mutable.HashMap

            // 迁入  迁出 初始化
            scenicOfSelectLacCi.foreach((s: (String, ScenicInfo)) => {
              userCheckIn.update(s, 0)
              userCheckOut.update(s, 0)
              // userCheckAll.update(s, (0, 0, 0))
            })

            // 用户进出景区记录列表(号码，景区，进出标志（1进0出），记录时间)
            var inAndOutRecord: List[(String, String, Int, Long)] = List()

            // 返回     (用户,当前景区)
            val tmpUserLocationMap: mutable.HashMap[String, String] =
              getTourUserSceneryId(hbaseUtilExecutor, hbaseUtilConnExecutor, "b_yz_app_td_hbase:TourHistory", distinctRow)

            // phone_no，（start_time, phone_no, imei, lac, cell, up_flow, down_flow, data_type, start_time_long）
            partitionRow.foreach((line: (String, Row)) => {

              def updateSceneryStatus(sceneryId: String, hashMap: mutable.HashMap[(String, ScenicInfo), Int]): Unit = {
                //              println("updateSceneryStatus" + sceneryId)
                if (scenicOfSelectLacCi.contains(sceneryId)) {
                  //                println("scenicOfSelectLacCi.contains(sceneryId)")
                  val scenicInfo: ScenicInfo = scenicOfSelectLacCi(sceneryId)
                  if (hashMap.contains((sceneryId, scenicInfo))) {
                    val number = hashMap((sceneryId, scenicInfo))
                    hashMap.update((sceneryId, scenicInfo), number + 1)
                  }
                  else {
                    hashMap.update((sceneryId, scenicInfo), 1)
                  }
                }
              }

              val f: Row = line._2
              // 此人号码
              val phoneNo = f(1).toString
              // 此人基站
              val lacCi = f(3).toString.concat("-").concat(f(4).toString)
              // 记录时间
              val startTime = f(0).toString
              // 记录时间
              val startTimeLong: Long = f(8).toString.toLong
              // 如果此人在hbase 用户表(用户,当前景区)
              if (tmpUserLocationMap.contains(phoneNo)) {
                // 此人上次的景区
                val lastScenery = tmpUserLocationMap(phoneNo)
                // 如果此人这次在景区的基站  valueOfSelectLacCi（景区码表）
                if (valueOfSelectLacCi.contains(lacCi)) {
                  // 此人在哪个景区
                  val sceneryFromSelectLacCi: String = valueOfSelectLacCi(lacCi).sceneryId //valueOfSelectLacCi景区码表
                  // 如果这次跟上次不是一个景区
                  // 7077!=7076 or X
                  if (!sceneryFromSelectLacCi.equals(lastScenery)) {
                    // 更新hbase 用户表
                    tmpUserLocationMap.update(phoneNo, sceneryFromSelectLacCi)
                    // 如果上个记录是在景区，说明切换了景区   X代表不在景区
                    if (!"X".equals(lastScenery)) {
                      val out = startTime + "," + f(1).toString.substring(2) + "," + f(2) + "," +
                        Long.toHexString(Long.parseLong(f(3).toString)) + "," +
                        Long.toHexString(Long.parseLong(f(4).toString)) + "," +
                        f(5) + "," + f(6) + "," + "0" + "," + f(7) + "," + lastScenery
                      updateSceneryStatus(lastScenery, userCheckOut)
                      inAndOutRecord = inAndOutRecord :+ (phoneNo, lastScenery, 0, startTimeLong)
                      kafkaProducer.value.send(TARGET_TOPIC, out)
                    }

                    val in = startTime + "," + f(1).toString.substring(2) + "," + f(2) + "," +
                      Long.toHexString(Long.parseLong(f(3).toString)) + "," +
                      Long.toHexString(Long.parseLong(f(4).toString)) + "," +
                      f(5) + "," + f(6) + "," + "1" + "," + f(7) + "," + sceneryFromSelectLacCi
                    updateSceneryStatus(sceneryFromSelectLacCi, userCheckIn)
                    inAndOutRecord = inAndOutRecord :+ (phoneNo, sceneryFromSelectLacCi, 1, startTimeLong)
                    kafkaProducer.value.send(TARGET_TOPIC, in)
                  }
                }
                // 如果此人不在景区
                // 用户离开景区,当前不在景区
                else {
                  tmpUserLocationMap.update(phoneNo, "X")
                  // 如果用户这次不在景区，上次在某个景区，表示离开了上一次景区
                  if (!"X".equals(lastScenery)) {
                    // 拼接结果
                    val out = startTime + "," + f(1).toString.substring(2) + "," + f(2) + "," +
                      Long.toHexString(Long.parseLong(f(3).toString)) + "," +
                      Long.toHexString(Long.parseLong(f(4).toString)) + "," +
                      f(5) + "," + f(6) + "," + "0" + "," + f(7) + "," + lastScenery
                    updateSceneryStatus(lastScenery, userCheckOut)
                    inAndOutRecord = inAndOutRecord :+ (phoneNo, lastScenery, 0, startTimeLong)
                    kafkaProducer.value.send(TARGET_TOPIC, out)
                  }
                }
              }
              else {
                // 表示该人首次进入景区
                // 如果在上个批次的全量表与临时表都找不到此人
                // 如果此人此次进入景区
                if (valueOfSelectLacCi.contains(lacCi)) { //判断此人的基站是在景区里面
                  val sceneryFromSelectLacCi = valueOfSelectLacCi(lacCi).sceneryId
                  tmpUserLocationMap.update(phoneNo, sceneryFromSelectLacCi)
                  // 拼接结果
                  // 业务流程开始时间，手机号，imei,lac,cell,上行流量，下行流量，进出景区标志（1进入，0离开），数据类型
                  val firstInOrOut = startTime + "," + f(1).toString.substring(2) + "," + f(2) +
                    "," + Long.toHexString(Long.parseLong(f(3).toString)) +
                    "," + Long.toHexString(Long.parseLong(f(4).toString)) +
                    "," + f(5) + "," + f(6) + "," + "1" + "," + f(7) + "," + sceneryFromSelectLacCi
                  updateSceneryStatus(sceneryFromSelectLacCi, userCheckIn)
                  inAndOutRecord = inAndOutRecord :+ (phoneNo, sceneryFromSelectLacCi, 1, startTimeLong)
                  kafkaProducer.value.send(TARGET_TOPIC, firstInOrOut)
                } else {
                  tmpUserLocationMap.update(phoneNo, "X")
                }
              }
            })

            //b_yz_app_td_hbase:TourHistory 用户与景区对应关系实时表
            hbaseUtilExecutor.putByKeyColumnList(hbaseUtilConnExecutor, "b_yz_app_td_hbase:TourHistory", "0", "0", tmpUserLocationMap.toList)

            // 维护用户最后所在基站信息（修复的时候使用） b_yz_app_td_hbase:UserLastStay	用户最后驻留基站位置实时表
            hbaseUtilExecutor.putByKeyColumnList(hbaseUtilConnExecutor, "b_yz_app_td_hbase:UserLastStay", "0", "0", userLastStayList)
            // 计算本分区内结果
            scenicOfSelectLacCi.foreach((pair: (String, ScenicInfo)) => {
              val checkInCount = userCheckIn(pair)
              val checkOutCount = userCheckOut(pair)
              val sum = checkInCount - checkOutCount
              // 景区Id，总数  迁入人数  迁出人数
              userCheckAll.update(pair, (sum, checkInCount, checkOutCount))
            })

            // userCheckAll.foreach(println)
            if (hbaseUtilConnExecutor != null) hbaseUtilConnExecutor.close()
            userCheckAll.toIterator
          })
          .reduceByKey((x, y) => (
            x._1 + y._1,
            x._2 + y._2,
            x._3 + y._3
          ))


        println("driver start:" + nowDate)

        // spark执行过程时间的不确定性，这里操作为了前端展现，将时间进行等距。
        val currentTimeMillis = getCurrentTimeMillis
        val timeDiff = currentTimeMillis - lastSendTime
        if (timeDiff > 10000) {
          lastSendTime = currentTimeMillis
        } else {
          lastSendTime = lastSendTime + 10000
        }
        val currentTime = timeMillsToDate(lastSendTime, format = "yyyy,MM,dd,HH,mm,ss")
        var sceneryCountList = List[(String, Int)]()
        // 本批次景区列表
        val thisPieceSubkeySet: mutable.Set[String] = scala.collection.mutable.Set[String]()
        // 本批次结果数据列表
        var resultList: List[(String, String, String)] = List()
        //  发布数据列表
        //      var publishList: List[(String, String)] = List()
        //      val publishDayJasonArray = new JSONArray()
        //      val publishCurrentJasonArray = new JSONArray()


        //      val collectedUserCheckAllItr: Array[((String, scenicInfo), (Int, Int, Int))] = userCheckAllItr.collect()
        //
        //      val sceneryCountResult = hun.getResultByKeyList(driverConn, "SceneryCount", collectedUserCheckAllItr.map(_._1._1).toSet)
        //        .map(kv => (kv._1, if (kv._2.isEmpty) 0 else Bytes.toString(kv._2.getValue("0".getBytes(), "0".getBytes())).toInt))

        userCheckAllItr.collect()
          .foreach(kv => {
            // 上批次驻留人数
            val sceneryInfo: (String, ScenicInfo) = kv._1
            val sceneryId = sceneryInfo._1 //景区ID
            val city = sceneryInfo._2.city // 景区归属地市
            val sceneryName = sceneryInfo._2.sceneryName //景区名称
            val alarmValue = sceneryInfo._2.alarmValue //景区人数告警阈值
            var alarmStatus = "正常" //状态

            val values = kv._2
            var scenerySum = values._1 //总数
            val sceneryCheckInSum = values._2 //迁入总数
            val sceneryCheckOutSum = values._3 //迁出总数

            // val countResult = sceneryCountResult(sceneryId)
            val countResult = hbaseUtilDriver.getValuesByKeyColumn(connDriver, "b_yz_app_td_hbase:SceneryCount", sceneryId, "0", "0")
            // 如果在Hbase中找到了此景区
            if (!countResult.isEmpty) {
              // 本批次变动人数
              scenerySum = try {
                scenerySum + Bytes.toString(countResult.getValue("0".getBytes(), "0".getBytes())).toInt
              }
              catch {
                case e => scenerySum
              }
            }
            // scenerySum = scenerySum + countResult

            if (scenerySum < 0) scenerySum = 0
            // 将景区人数插入List
            sceneryCountList = (sceneryId, scenerySum) +: sceneryCountList
            // sceneryCountResult.update(sceneryId, scenerySum)
            if (scenerySum > alarmValue.toInt) alarmStatus = "告警"
            // 添加景区id到本批次景区列表
            thisPieceSubkeySet.add(sceneryId)

            val jsonObject1 = new JSONObject()
            jsonObject1.put("calDate", currentTime)
            jsonObject1.put("peopleNum", scenerySum)
            jsonObject1.put("moveInNum", sceneryCheckInSum)
            jsonObject1.put("moveOutNum", sceneryCheckOutSum)

            resultList = ("td:travel:realtime:day:" + sceneryId, currentTime, jsonObject1.toJSONString) +: resultList

            // jsonObject1.put("sceneryId", sceneryId)
            // jsonObject1.put("currentTime", currentTime)

            // publishDayJasonArray.add(jsonObject1)

            val jsonObject2 = new JSONObject()
            jsonObject2.put("peopleNum", scenerySum)
            jsonObject2.put("placeId", sceneryId)
            jsonObject2.put("moveInNum", sceneryCheckInSum)
            jsonObject2.put("moveOutNum", sceneryCheckOutSum)
            jsonObject2.put("placeName", sceneryName)
            jsonObject2.put("status", alarmStatus)

            resultList = ("td:travel:realtime:current:" + city, sceneryId, jsonObject2.toJSONString) +: resultList

            // jsonObject2.put("city", city)
            // jsonObject2.put("sceneryId", sceneryId)

            // publishCurrentJasonArray.add(jsonObject2)
          })

        val TD_TRAVEL_SCENERY_ID_SET = travelSceneryMap.map(_._1).toSet

        // 判断是否有删除的景区
        println("thisPieceSubkeySet:" + thisPieceSubkeySet.size)
        val diffSet = TD_TRAVEL_SCENERY_ID_SET diff thisPieceSubkeySet
        //          如果本批次数据中没有redis中存储的景区,删除redis中的数据
        if (diffSet.size > 0) {
          println("delete scenerySet is " + diffSet)
          diffSet.foreach(sceneryId => {
            val sceneryCity: String = travelSceneryMap(sceneryId)
            // 删除redis中多出的key   hdel(key, field)：删除名称为key的hash中键为field的域
            jedisDmz.hdel(sceneryCity, sceneryId)
            // 删除TD_TRAVEL_SCENERY_ID_MAP中多出的key
            travelSceneryMap.remove(sceneryId)
          })
        }

        // 景区实时人数表
        hbaseUtilDriver.putByKeyColumnListInt(connDriver, "b_yz_app_td_hbase:SceneryCount", sceneryCountList)


        setRedisResultList(jedisDmz, resultList)
        // pubRedisResult(jedisDmz, "td:travel:realtime:day:channel", publishDayJasonArray.toJSONString)
        // pubRedisResult(jedisDmz, "td:travel:realtime:current:channel", publishCurrentJasonArray.toJSONString)
        println("driver end:" + nowDate)
        //        if(connDriver != null) connDriver.close()
      })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
