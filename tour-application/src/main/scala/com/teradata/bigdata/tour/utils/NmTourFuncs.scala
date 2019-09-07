package com.teradata.bigdata.tour.utils

import com.alibaba.fastjson.JSON
import com.teradata.bigdata.tour.bean.{SceneryLastInfo, ScenicInfo, UserInfo}
import com.teradata.bigdata.util.redis.RedisUtil
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable

trait NmTourFuncs extends Serializable {

  def getDmzRedisConnect = {
    val host = "10.221.156.232"
    val port = 8021
    new Jedis(host, port)
  }

  /**
    * 景区的配置码表
    * @param conn
    * @return
    */
  def getSelectLacCiAllRows(conn: java.sql.Connection): collection.Map[String, ScenicInfo] = {
    val newMap: collection.mutable.Map[String, ScenicInfo] = new scala.collection.mutable.HashMap
    // SCENERY_ID 景区ID   ALARM_VALUE 景区内人数告警阈值
    val scenicSql = "SEL LAC_ID,CELL_ID,SCENERY_ID,CITY_ID,SCENERY_NAME,ALARM_VALUE FROM PVIEW.VW_MID_SCENERY_INFO_ALARM"
    val preScenicSql = conn.prepareStatement(scenicSql)
    val scenicResult = preScenicSql.executeQuery()
    while (scenicResult.next()) {
      val lacId = Integer.parseInt(scenicResult.getString("LAC_ID"), 16)
      val cellId = Integer.parseInt(scenicResult.getString("CELL_ID"), 16)
      val sceneryId = scenicResult.getString("SCENERY_ID")
      val cityId = scenicResult.getString("CITY_ID")
      val sceneryName = scenicResult.getString("SCENERY_NAME")
      val alarmValue = scenicResult.getString("ALARM_VALUE")
      val lacCellId = lacId + "-" + cellId
      // 可能存在一个基站归属于两个景区的情况，这里人为的把放到一个景区
      newMap.update(lacCellId, ScenicInfo(sceneryId, sceneryName, cityId, alarmValue))
    }
    newMap
  }

  def getSceneryInfoFromDmz(jedis: Jedis): mutable.HashMap[String, String] = {
    val currentKeys = jedis.keys("td:travel:realtime:current:*")
    val TD_TRAVEL_SCENERY_ID_MAP: mutable.HashMap[String, String] = new mutable.HashMap
    for (key <- currentKeys) {
      val fieldValues = mapAsScalaMap(jedis.hgetAll(key))
      fieldValues.foreach(fv => {
        TD_TRAVEL_SCENERY_ID_MAP.update(fv._1, key)
      })
    }
    TD_TRAVEL_SCENERY_ID_MAP
  }


  def getRedisConnect(): Jedis = {
    val redisUtil = new RedisUtil
    redisUtil.getRedisConnect
  }

  def getRedisUserInfo(jedis: Jedis, key: String, fieldList: List[String]): mutable.HashMap[String, UserInfo] = {
    val redisUtil = new RedisUtil
    redisUtil
      .hGetRedisPip(jedis, key, fieldList)
      .map(map => {
        val phoneNo = map._1
        val userInfo = map._2
        if (userInfo != null) {
          val userInfoText = userInfo.toString

          val userInfoArr = userInfoText.split(",")
          //      val city=json.get("city").toString

          val msisdn = userInfoArr(0)
          //        val innetDate = userInfoArr(1)
          //        val subsId = userInfoArr(2)
          //        val subsStsEfctDate = userInfoArr(3)
          val personCustLevel = userInfoArr(4)
          val custLevelTitle = userInfoArr(5)
          val consumeAmt = userInfoArr(6)
          val arpuLevel = userInfoArr(7)
          val arpuLevelTitle = userInfoArr(8)
          val acctTypeCode = userInfoArr(9)
          val acctTypeTitle = userInfoArr(10)
          val genderCode = userInfoArr(11)
          val genderTitle = userInfoArr(12)
          //        val channelId = userInfoArr(13)
          //        val channelTitle = userInfoArr(14)
          val imsi = userInfoArr(15)
          val birthday = userInfoArr(16)
          val ageLevelCode = userInfoArr(17)
          val ageLevelTitle = userInfoArr(18)
          val ageOld = userInfoArr(19)
          //        val custId = userInfoArr(20)
          //        val ratePlanId = userInfoArr(21)
          //        val ratePlanTitle = userInfoArr(22)
          val occuTypeId = userInfoArr(23)
          val occuTypeTitle = userInfoArr(24)
          //        val calDate = userInfoArr(25)
          //        val majorCardFlag = userInfoArr(26)
          //        val majorCardFlagTitle = userInfoArr(27)
          val cityId = userInfoArr(28)
          val cityName = userInfoArr(29)

          (phoneNo, UserInfo(
            msisdn
            , personCustLevel
            , custLevelTitle
            , consumeAmt
            , arpuLevel
            , arpuLevelTitle
            , acctTypeCode
            , acctTypeTitle
            , genderCode
            , genderTitle
            , imsi
            , birthday
            , ageLevelCode
            , ageLevelTitle
            , ageOld
            , occuTypeId
            , occuTypeTitle
            , cityId
            , cityName))
        } else {
          (phoneNo, null)
        }

      })
  }


  def getRedisUserCurrentLocation(jedis: Jedis, tableName: String, fieldList: List[String]): mutable.HashMap[String, String] = {
    val redisUtil = new RedisUtil
    redisUtil
      .hGetRedisPip(jedis, tableName, fieldList)
      .filter(_._2 != null)
      .map(kv => {
        (kv._1, kv._2.toString)
      })

  }

  def setRedisUserCurrentLocation(jedis: Jedis, tableName: String, fieldList: List[(String, String)]) {
    val redisUtil = new RedisUtil
    redisUtil
      .hSetRedisPip(jedis, tableName, fieldList)
  }

  def setRedisSceneryCurrentCitys(jedis: Jedis, tableName: String, fieldList: List[(String, String)]): Unit = {
    val redisUtil = new RedisUtil
    redisUtil.hSetRedisPip(jedis, tableName, fieldList)
  }

  def setRedisResultList(jedis: Jedis, fieldList: List[(String, String, String)]): Unit = {
    val redisUtil = new RedisUtil
    redisUtil.hSetRedisPip(jedis, fieldList)
  }

  def pubRedisResultList(jedis: Jedis, messageList: List[(String, String)]): Unit = {
    val redisUtil = new RedisUtil
    redisUtil.publishMessageRedisPip(jedis, messageList)
  }

  def pubRedisResult(jedis: Jedis, channelName: String, message: String): Unit = {
    val redisUtil = new RedisUtil
    redisUtil.publistMessage(jedis, channelName, message)
  }

  /*def getRedisSceneryCurrentCitys(jedis: Jedis, tableName: String): List[SceneryLastInfo] = {
    val redisUtil = new RedisUtil
    var resultList: List[SceneryLastInfo] = List()
    redisUtil.hGetAllRedisPip(jedis, tableName).foreach(kv => {
      val sceneryId = kv._1
      val sceneryCitysInfo = kv._2
      //      val json="\{\"471\": 20, \"472\": 3, \"479\": 20\}"

      val sceneryStatusJson = JSON.parseObject(sceneryCitysInfo)
      val sceneryName = sceneryStatusJson.getString("sceneryName")
      val sceneryLastSum = sceneryStatusJson.getInteger("scenerySum")
      val sceneryAlarmValue = sceneryStatusJson.getInteger("sceneryAlarmValue")
      val cityWhereSceneryBelong = sceneryStatusJson.getString("cityWhereSceneryBelong")
      val SceneryCityInfoList = JSON.parseArray(sceneryStatusJson.get("sceneryCityList").toString).toSet

      //      asScalaSet(SceneryCityInfoList)
      SceneryCityInfoList.foreach(info => {
        val infoJsonObeject = JSON.parseObject(info.toString)
        val cityId = infoJsonObeject.get("cityId").toString
        val cityName = infoJsonObeject.get("cityName").toString
        val cityCurrentValue = infoJsonObeject.get("currentValue").toString.toInt
        resultList = SceneryLastInfo(sceneryId, sceneryName, sceneryLastSum, sceneryAlarmValue, cityWhereSceneryBelong, cityId, cityName, cityCurrentValue) +: resultList
      })
    })
    resultList
  }*/

  def getRedisSceneryCurrentCitys(jedis: Jedis, tableName: String, fieldList: List[String]): List[SceneryLastInfo] = {
    val redisUtil = new RedisUtil
    var resultList: List[SceneryLastInfo] = List()

    redisUtil.hGetRedisPip(jedis, tableName, fieldList).foreach(kv => {
      val sceneryId = kv._1
      val sceneryCitysInfo = kv._2
      if (sceneryCitysInfo != null) {
        //        println("kv"+kv)
        val sceneryStatusJson = JSON.parseObject(sceneryCitysInfo.toString)
        val sceneryName = sceneryStatusJson.getString("sceneryName")
        val sceneryLastSum = sceneryStatusJson.getInteger("scenerySum")
        val sceneryAlarmValue = sceneryStatusJson.getInteger("sceneryAlarmValue")
        val cityWhereSceneryBelong = sceneryStatusJson.getString("cityWhereSceneryBelong")

        var cityList: List[(String, String, Int)] = List()

        JSON.parseArray(sceneryStatusJson.get("sceneryCityList").toString)
          .map(_.toString)
          .toSet
          .foreach((info: String) => {
            val infoJsonObeject = JSON.parseObject(info)
            val cityId = infoJsonObeject.get("cityID").toString
            val cityName = infoJsonObeject.get("cityName").toString
            val cityCurrentValue = infoJsonObeject.get("currentValue").toString.toInt
            //          resultList = SceneryLastInfo(sceneryId, sceneryName, sceneryLastSum, sceneryAlarmValue, cityWhereSceneryBelong, cityId, cityName, cityCurrentValue) +: resultList
            cityList = (cityId, cityName, cityCurrentValue) +: cityList
          })
        resultList = SceneryLastInfo(sceneryId, sceneryName, sceneryLastSum, sceneryAlarmValue, cityWhereSceneryBelong, cityList) +: resultList

      }
    })
    resultList
  }


  def getRedisSceneryCurrentSum(jedis: Jedis, tableName: String): List[(String, Int)] = {
    val redisUtil = new RedisUtil
    redisUtil.hGetAllRedisPip(jedis, tableName).map(kv => {
      val sceneryId = kv._1
      val scenerySum = kv._2.toInt
      (sceneryId, scenerySum)
    }).toList
  }

  def getTourUserSceneryId(hun: HbaseUtilNew
                           , hbaseConn: Connection
                           , tableName: String
                           , keyList: Set[String]) = {
    val UserSceneryIdMap: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap

    hun.getResultByKeyList(hbaseConn, tableName, keyList).foreach(kv => {
      val phoneNo = kv._1
      val result = kv._2
      val cells: Array[Cell] = result.rawCells()
      cells.foreach(cell => {
        val phone = Bytes.toString(CellUtil.cloneRow(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        UserSceneryIdMap.update(phone, value)
      })
    })
    UserSceneryIdMap
  }

  def getPartitionPhoneNo(phoneNo: String): String = {
    try {
      phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
    }
    catch {
      case e: Exception => println("substring problem phoneno:" + phoneNo)
        phoneNo
    }
  }


  /*def getSelectLacCiAllRows: collection.Map[String, scenicInfo] = {
    val newMap: collection.mutable.Map[String, scenicInfo] = new scala.collection.mutable.HashMap
    val selectLacCi: collection.Map[String, (String, String, String, String)] =
      hunDriver.getAllRows_TOUR("b_yz_app_td_hbase:SelectLacCi", "0")
    selectLacCi.foreach(m => {
      var lac = 0
      var cell = 0
      val sceneryInfo = m._2
      val sceneryId = sceneryInfo._1
      val sceneryName = sceneryInfo._2
      val city = sceneryInfo._3
      val alarmValue = sceneryInfo._4

      try {
        lac = Integer.parseInt(m._1.split("-")(0), 16)
        cell = Integer.parseInt(m._1.split("-")(1), 16)
        newMap.update(lac + "-" + cell, scenicInfo(sceneryId, sceneryName, city, alarmValue))
      } catch {
        case e: NumberFormatException => println("基站信息错误：" + m._1)
      }

    })
    newMap
  }*/

}
