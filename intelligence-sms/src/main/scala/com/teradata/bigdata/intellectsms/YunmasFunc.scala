package com.teradata.bigdata.intellectsms

import com.teradata.bigdata.intellectsms.users.YunmasActInfo
import com.teradata.bigdata.util.gbase.GbaseConnect
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.kafka.KafkaSink
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/28/028 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait YunmasFunc extends TimeFuncs with GbaseConnect with Serializable {

  val loggger: Logger = org.apache.log4j.LogManager.getLogger("YunmasFunc")

  @Deprecated
  def getPartyUsersFromHbase(hbaseUtil: HbaseUtil): Set[String] = {
    var ResultSet = Set[String]()
    hbaseUtil.getAllRows("DHXSelectPhoneList").foreach(r => {
      ResultSet = ResultSet.+(Bytes.toString(r.getRow))
    })
    ResultSet
  }

  /**
    * 从gbase获取活动配置信息
    *
    * @return
    */
  def getYunmasActs: mutable.HashMap[String, YunmasActInfo] = {
    val conn = gbaseOutterConnect
    //    val currentDate = toDate(getCurrentTimeMillis)
    val resultMap: mutable.HashMap[String, YunmasActInfo] = new mutable.HashMap

    //    project_seq
    //    cloud_org_name
    //    cloud_org_sms_name
    //    rule_id
    //    rule_name
    //    rule_area_type
    //    rule_area_code
    //    rule_lac_id_cell_id
    //    rule_need_id
    //    rule_need
    //    rule_delete_id
    //    rule_delete
    //    flag_delete_longstaymsisdn
    //    longstaymsisdn_laccell
    //    stay_minutes
    val configSql = "select * from pview.vw_cloudmas_rule_to_td"
    val preCustSql = conn.prepareStatement(configSql)
    val configResult = preCustSql.executeQuery()
    while (configResult.next()) {
      val actId = configResult.getString("rule_id")
      //  活动要求用户当前所在城市
      val localCity = configResult.getString("rule_area_code")
      var localCitySet: Set[String] = Set()
      if (localCity != null) {
        localCitySet = if (localCity.matches("\\s*")) Set[String]() else localCity.split(",", -1).toSet
      }

      //      活动要求用户当前所在基站列表范围
      val lacCellString = configResult.getString("rule_lac_id_cell_id")
      var lacCellSet:Set[String] = Set()
      if (lacCellString != null) {
        lacCellSet = if (lacCellString.matches("\\s*")) Set[String]() else lacCellString.split(",").toSet
      }
      if (lacCellSet.size > 0 || localCitySet.size > 0) {

        /*
        东华信通的映射关系：
        1 本地用户       ==>4
        2 常驻用户
        3 省内漫游用户   ==>3
        4 省际漫游用户   ==>2
        5 国际漫游用户   ==>1
        6 边界漫游用户
         */
        /*真实信令数据--用户漫游类型：
        1：国际漫游_
        2：省际漫游_
        3：省内漫游_
        4：本地*/
        val roamTypeString = configResult.getString("rule_need_id")
        if (roamTypeString != null) {
          var roamTypeSet = if (roamTypeString.matches("\\s*")) Set[String]()
          else roamTypeString.split("\\|", -1)
            .filter(Set("1", "3", "4", "5").contains(_))
            .toSet
          if (roamTypeSet.size == 4) roamTypeSet = Set()

          roamTypeSet = roamTypeSet.map(t => {
            t match {
              case "1" => "4"
              case "3" => "3"
              case "4" => "2"
              case "5" => "1"
            }
          })

          //  用户驻留时长，Gbase存储的单位是分钟，信令精度是毫秒，hbase存储的精度是秒
          val stayDurationString = configResult.getLong("stay_minutes")
          if (stayDurationString != null) {
            val stayDuration = stayDurationString * 60
            //  活动要求所在的城市
            //  活动要求所在的基站
            //  漫游类型
            //  驻留时长
            resultMap.update(actId, YunmasActInfo(localCitySet, lacCellSet, roamTypeSet, stayDuration))
          }
        }
      }
    }
    if (conn != null) conn.close()
    resultMap
  }

  /**
    *
    * @param hun
    * @param conn
    * @param keyList
    * @return
    */
  def getYunmasUserBaseInfo(hun: HbaseUtil
                            , conn: Connection
                            , keyList: Set[String]) : mutable.HashMap[String, (String, String)] = {
    val userBaseInfoResultMap: mutable.HashMap[String, (String, String)] = new mutable.HashMap
    hun.getResultByKeyList(conn, "YunmasUserBaseInfo", keyList).foreach(kv => {
      if (!kv._2.isEmpty) {
        val phoneNo = kv._1
        val userBaseInfoResult = kv._2
        val lacCellList = Bytes.toString(userBaseInfoResult.getValue("0".getBytes(), "0".getBytes()))
        val country = Bytes.toString(userBaseInfoResult.getValue("0".getBytes(), "1".getBytes()))
        userBaseInfoResultMap.update(phoneNo, (lacCellList, country))
      }
    })
    userBaseInfoResultMap
  }

  /**
    * 返回结果：手机号最后2位+手机号
    *
    * @param phoneNo
    * @return
    */
  def getPartitionPhoneNo(phoneNo: String): String = {
    phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
  }

  def judgeConditionsAndSendTest(kafkaProducer: KafkaSink[String, String]
                             , targetTopic: String
                             , userCurrentInfo: ((String, Long), String, String, String, String, String, String, String)
                             , actId: String
                             , yunmasActInfo: YunmasActInfo
                             , yunmasUserLastStatus: mutable.HashMap[String, (String, Long, Long)]
                             , log: Logger
                            ): Boolean ={

    val userPhoneNo = userCurrentInfo._2
    val userLocalCity = userCurrentInfo._3
    val userRoamType = userCurrentInfo._4
    val userOwnerProvince = userCurrentInfo._5
    val userOwnerCity = userCurrentInfo._6
    val lac = userCurrentInfo._7
    val ci = userCurrentInfo._8
    val userLacCell = lac + "-" + ci
    val userStartTime: String = userCurrentInfo._1._1
    val userStartTimeLong: Long = userCurrentInfo._1._2


    val stringLine = userStartTime + "|" +
      userPhoneNo + "|" +
      userLocalCity + "|" +
      userRoamType + "|" +
      userOwnerProvince + "|" +
      userOwnerCity + "|" +
      lac + "|" +
      ci

    var sendFlag = false
    var isContinue = true

    if (yunmasUserLastStatus.contains(userPhoneNo)) {
      val lastStatus: (String, Long, Long) = yunmasUserLastStatus(userPhoneNo)

      if (generalJudgeFunc(yunmasActInfo, userLocalCity, userLacCell, userRoamType)) {
        //                    用户上批次驻留时间(秒)
        val lastDuration = lastStatus._3
        //                    用户上批次驻留开始时间(秒)
        val lastStartTime = lastStatus._2
        //                  startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
        val newDuration = userStartTimeLong / 1000 - lastStartTime + lastDuration
        //                    当此用户驻留时间超过1个小时
        if (newDuration >= yunmasActInfo.stayDuration) {
          sendFlag = true
        }
        yunmasUserLastStatus.update(userPhoneNo, (actId, userStartTimeLong / 1000, newDuration))
        isContinue = false
      }
    }else {
      yunmasUserLastStatus.update(userPhoneNo, ("X", userStartTimeLong / 1000, 0))
    }

    if (sendFlag) {
      send(kafkaProducer, targetTopic, actId, stringLine)
    }
    isContinue
  }

  def judgeConditionsAndSendDuration(kafkaProducer: KafkaSink[String, String]
                                 , targetTopic: String
                                 , userCurrentInfo: ((String, Long), String, String, String, String, String, String, String)
                                 , actId: String
                                 , yunmasActInfo: YunmasActInfo
                                 , yunmasUserLastStatus: mutable.HashMap[String, (String, Long, Long)]
                                ): Boolean ={

    val userPhoneNo = userCurrentInfo._2
    val userLocalCity = userCurrentInfo._3
    val userRoamType = userCurrentInfo._4
    val userOwnerProvince = userCurrentInfo._5
    val userOwnerCity = userCurrentInfo._6
    val lac = userCurrentInfo._7
    val ci = userCurrentInfo._8
    val userLacCell = lac + "-" + ci
    val userStartTime: String = userCurrentInfo._1._1
    val userStartTimeLong: Long = userCurrentInfo._1._2


    val stringLine = userStartTime + "|" +
      userPhoneNo + "|" +
      userLocalCity + "|" +
      userRoamType + "|" +
      userOwnerProvince + "|" +
      userOwnerCity + "|" +
      lac + "|" +
      ci

    var sendFlag = false
    var isContinue = true

    if (yunmasUserLastStatus.contains(userPhoneNo)) {
      val lastStatus: (String, Long, Long) = yunmasUserLastStatus(userPhoneNo)

      if (generalJudgeFunc(yunmasActInfo, userLocalCity, userLacCell, userRoamType)) {
        //                    用户上批次驻留时间(秒)
        val lastDuration = lastStatus._3
        //                    用户上批次驻留开始时间(秒)
        val lastStartTime = lastStatus._2
        //                  startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
        val newDuration = userStartTimeLong / 1000 - lastStartTime + lastDuration
        //                    当此用户驻留时间超过1个小时
        if (newDuration >= yunmasActInfo.stayDuration) {
          sendFlag = true
        }
        yunmasUserLastStatus.update(userPhoneNo, (actId, userStartTimeLong / 1000, newDuration))
        isContinue = false
      }
    }else {
      yunmasUserLastStatus.update(userPhoneNo, ("X", userStartTimeLong / 1000, 0))
    }

    if (sendFlag) {
      send(kafkaProducer, targetTopic, actId, stringLine)
    }
    isContinue
  }

  /**
    * 根据活动配置要求的位置信息和驻留时长判断是否发送信息
    * @param kafkaProducer          kafka生产者
    * @param targetTopic            发送目标topic
    * @param userCurrentInfo        用户当前位置信息状态
    * @param actId                  需求ID
    * @param yunmasActInfo          从gbase获取活动配置 pview.vw_cloudmas_rule_to_td （活动要求用户当前所在城市、活动要求用户当前所在基站列表范围、要求的漫游类型、要求的驻留时长(秒)）
    * @param yunmasUserLastStatus   从hbase取出用户的上批次最后状态
    */
  def judgeConditionsAndSendNew(kafkaProducer: KafkaSink[String, String]
                                 , targetTopic: String
                                 , userCurrentInfo: ((String, Long), String, String, String, String, String, String, String)
                                 , actId: String
                                 , yunmasActInfo: YunmasActInfo
                                ): Unit ={

    val userPhoneNo = userCurrentInfo._2
    val userLocalCity = userCurrentInfo._3
    val userRoamType = userCurrentInfo._4
    val userOwnerProvince = userCurrentInfo._5
    val userOwnerCity = userCurrentInfo._6
    val lac = userCurrentInfo._7
    val ci = userCurrentInfo._8
    val userLacCell = lac + "-" + ci
    val userStartTime: String = userCurrentInfo._1._1

    val stringLine = userStartTime + "|" +
      userPhoneNo + "|" +
      userLocalCity + "|" +
      userRoamType + "|" +
      userOwnerProvince + "|" +
      userOwnerCity + "|" +
      lac + "|" +
      ci

    val sendFlag = generalJudgeFunc(yunmasActInfo, userLocalCity, userLacCell, userRoamType)

    if (sendFlag) {
      send(kafkaProducer, targetTopic, actId, stringLine)
    }
  }


  /**
    * 根据活动配置要求的位置信息和驻留时长判断是否发送信息
    * @param kafkaProducer          kafka生产者
    * @param targetTopic            发送目标topic
    * @param userCurrentInfo        用户当前位置信息状态
    * @param actId                  需求ID
    * @param yunmasActInfo          从gbase获取活动配置 pview.vw_cloudmas_rule_to_td （活动要求用户当前所在城市、活动要求用户当前所在基站列表范围、要求的漫游类型、要求的驻留时长(秒)）
    * @param yunmasUserLastStatus   从hbase取出用户的上批次最后状态
    */
  def judgeConditionsAndSend(kafkaProducer: KafkaSink[String, String]
                             , targetTopic: String
                             , userCurrentInfo: ((String, Long), String, String, String, String, String, String, String)
                             , actId: String
                             , yunmasActInfo: YunmasActInfo
                             , yunmasUserLastStatus: mutable.HashMap[String, (String, Long, Long)]
                            ): Unit = {

    val userPhoneNo = userCurrentInfo._2
    val userLocalCity = userCurrentInfo._3
    val userRoamType = userCurrentInfo._4
    val userOwnerProvince = userCurrentInfo._5
    val userOwnerCity = userCurrentInfo._6
    val lac = userCurrentInfo._7
    val ci = userCurrentInfo._8
    val userLacCell = lac + "-" + ci
    val userStartTime: String = userCurrentInfo._1._1
    val userStartTimeLong: Long = userCurrentInfo._1._2


    val stringLine = userStartTime + "|" +
      userPhoneNo + "|" +
      userLocalCity + "|" +
      userRoamType + "|" +
      userOwnerProvince + "|" +
      userOwnerCity + "|" +
      lac + "|" +
      ci

    var sendFlag = false

    if (yunmasActInfo.stayDuration != 0) {
      //      如果判断驻留时长
      sendFlag = judgeUserStayDuration(
        generalJudgeFunc(yunmasActInfo, userLocalCity, userLacCell, userRoamType)
        , yunmasActInfo.stayDuration
        , userStartTimeLong
        , userPhoneNo
        , actId
        , yunmasUserLastStatus
      )
    } else {
      //      如果不判断驻留时长，判断完毕直接发送
      sendFlag = generalJudgeFunc(yunmasActInfo, userLocalCity, userLacCell, userRoamType)
    }

    if (sendFlag) {
      send(kafkaProducer, targetTopic, actId, stringLine)
    }
  }

  /**
    * 判断用户当前所在城市，是否在基站范围内，是否在漫游类型范围内
    * @param yunmasActInfo    从gbase获取活动配置 pview.vw_cloudmas_rule_to_td （活动要求用户当前所在城市、活动要求用户当前所在基站列表范围、要求的漫游类型、要求的驻留时长(秒)）
    * @param userLocalCity    信令中当前用户所在地市
    * @param userLacCell      信令中当前用户所在cell信息
    * @param userRoamType     信令中当前用户的漫游类型
    * @return
    */
  def generalJudgeFunc(yunmasActInfo: YunmasActInfo
                       , userLocalCity: String
                       , userLacCell: String
                       , userRoamType: String): Boolean = {
    var isFlag1 = true
    var isFlag2 = true
    var isFlag3 = true
    /*
            东华信通的映射关系：
            1 本地用户       ==>4
            2 常驻用户
            3 省内漫游用户   ==>3
            4 省际漫游用户   ==>2
            5 国际漫游用户   ==>1
            6 边界漫游用户
             */
    if (yunmasActInfo.localCitySet.size > 0 && !yunmasActInfo.localCitySet.contains(userLocalCity)) isFlag1 = false

    if (yunmasActInfo.lacCellSet.size > 0 && !yunmasActInfo.lacCellSet.contains(userLacCell)) isFlag2 = false

    if (yunmasActInfo.roamTypeSet.size > 0 && !yunmasActInfo.roamTypeSet.contains(userRoamType)) isFlag3 = false

    isFlag1 && isFlag2 && isFlag3
  }


  /**
    * 根据驻留时长判断是否发送消息
    *
    * @param generalJudgeFunc     判断用户当前所在城市，是否在基站范围内，是否在漫游类型范围内
    * @param stayDuration         驻留时长
    * @param userStartTimeLong    进入基站开始时间
    * @param userPhoneNo          用户手机号
    * @param yunmasUserLastStatus 用户上批状态
    * @return
    */
  def judgeUserStayDuration(generalJudgeFunc: Boolean
                            , stayDuration: Long
                            , userStartTimeLong: Long
                            , userPhoneNo: String
                            , actId: String
                            , yunmasUserLastStatus: mutable.HashMap[String, (String, Long, Long)]
                           ): Boolean = {
    var sendFlag = false
    if (yunmasUserLastStatus.contains(userPhoneNo)) {
      val lastStatus: (String, Long, Long) = yunmasUserLastStatus(userPhoneNo)
      val lastEventType = lastStatus._1

      if (generalJudgeFunc) {
        //                    用户上批次驻留时间(秒)
        val lastDuration = lastStatus._3
        //                    用户上批次驻留开始时间(秒)
        val lastStartTime = lastStatus._2
        //                  startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
        val newDuration = userStartTimeLong / 1000 - lastStartTime + lastDuration
        //                    当此用户驻留时间超过1个小时
        if (newDuration >= stayDuration) {
          sendFlag = true
        }
        yunmasUserLastStatus.update(userPhoneNo, (lastEventType, userStartTimeLong / 1000, newDuration))
//        val an: List[(String, (String, Long, Long))] = yunmasUserLastStatus.toList
//        hbaseUtil.putByKeyColumnList_MAS(connection,"b_yz_app_td_hbase:anliang",an)
      } else {
        yunmasUserLastStatus.update(userPhoneNo, ("X", userStartTimeLong / 1000, 0))
      }
    }else{
      if (generalJudgeFunc) {
        yunmasUserLastStatus.update(userPhoneNo, (actId, userStartTimeLong / 1000, 0))
      }
    }
    sendFlag
  }

  /**
    * kafka消息发送
    *
    * @param kafkaProducer 生产者
    * @param targetTopic   目标Topic
    * @param eventType     需求ID
    * @param stringLine    发送内容
    */
  def send(kafkaProducer: KafkaSink[String, String], targetTopic: String, eventType: String, stringLine: String): Unit = {
    kafkaProducer.send(targetTopic, eventType.toString + "|" + stringLine)
  }

  /**
    * 从hbase表获取指定用户的驻留信息
    *
    * @param hbaseUtil
    * @param conn
    * @param partitionPhoneNos rowkey用户手机号列表
    * @return
    */
  def getYunmasUserLastStatus(hbaseUtil: HbaseUtil, conn: Connection, partitionPhoneNos: List[String]): mutable.HashMap[String, (String, Long, Long)] = {
    hbaseUtil.getResultByKeyList_MAS(conn, "b_yz_app_td_hbase:TourMasUserNew", partitionPhoneNos)
  }

  def getYunmasUserLastStatusTest(hbaseUtil: HbaseUtil, conn: Connection, partitionPhoneNos: List[String]): mutable.HashMap[String, (String, Long, Long)] = {
    hbaseUtil.getResultByKeyList_MAS(conn, "b_yz_app_td_hbase:anliang", partitionPhoneNos)
  }

  /**
    * 更新新进入计时区域和驻留时长更新区域的用户，同时删除离开的用户
    * @param hun
    * @param conn
    * @param yunmasUserLastStatus
    */
  def updateAndDeleteUserStatus(hbaseUtil: HbaseUtil, conn: Connection, yunmasUserLastStatus: mutable.Map[String, (String, Long, Long)]): Unit = {
    val putResultList: List[(String, (String, Long, Long))] = yunmasUserLastStatus.filter(!_._2._1.equals("X")).toList
    hbaseUtil.putByKeyColumnList_MAS(conn, "b_yz_app_td_hbase:TourMasUserNew", putResultList)
    // 删除hbase已经离开的用户
    val delResultList = yunmasUserLastStatus.filter(_._2._1.equals("X")).map(_._1).toList
    hbaseUtil.deleteRows(conn, "b_yz_app_td_hbase:TourMasUserNew", delResultList)
  }

  /**
    * 更新新进入计时区域和驻留时长更新区域的用户，同时删除离开的用户
    * @param hbaseUtil
    * @param conn
    * @param yunmasUserLastStatus
    */
  def updateAndDeleteUserStatusNew(hbaseUtil: HbaseUtil, conn: Connection, yunmasUserLastStatus: mutable.Map[String, (String, Long, Long)]): Unit = {
    val putResultList: List[(String, (String, Long, Long))] = yunmasUserLastStatus.filter(!_._2._1.equals("X")).toList
    hbaseUtil.putByKeyColumnList_MAS(conn, "b_yz_app_td_hbase:TourMasUserNew", putResultList)
    // 删除hbase已经离开的用户
    val delResultList = yunmasUserLastStatus.filter(_._2._1.equals("X")).map(_._1).toList
    hbaseUtil.deleteRows(conn, "b_yz_app_td_hbase:TourMasUserNew", delResultList)
  }

  def updateAndDeleteUserStatusDuration(hbaseUtil: HbaseUtil, conn: Connection, yunmasUserLastStatus: mutable.Map[String, (String, Long, Long)]): Unit = {
    val putResultList: List[(String, (String, Long, Long))] = yunmasUserLastStatus.filter(!_._2._1.equals("X")).toList
    hbaseUtil.putByKeyColumnList_MAS(conn, "b_yz_app_td_hbase:TourMasUserNew", putResultList)
    // 删除hbase已经离开的用户
    val delResultList = yunmasUserLastStatus.filter(_._2._1.equals("X")).map(_._1).toList
    hbaseUtil.deleteRows(conn, "b_yz_app_td_hbase:TourMasUserNew", delResultList)
  }

  def updateAndDeleteUserStatusTest(hbaseUtil: HbaseUtil, conn: Connection, yunmasUserLastStatus: mutable.Map[String, (String, Long, Long)]): Unit = {
    val putResultList: List[(String, (String, Long, Long))] = yunmasUserLastStatus.filter(!_._2._1.equals("X")).toList

    loggger.info("putResultList@put" + putResultList.size)
    hbaseUtil.putByKeyColumnList_MAS(conn, "b_yz_app_td_hbase:anliang", putResultList)
    // 删除hbase已经离开的用户
    val delResultList = yunmasUserLastStatus.filter(_._2._1.equals("X")).map(_._1).toList
    loggger.info("delResultList@put" + delResultList.size)
    hbaseUtil.deleteRows(conn, "b_yz_app_td_hbase:anliang", delResultList)
  }
}
