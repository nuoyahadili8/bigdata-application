package com.teradata.bigdata.tour.utils

import java.util

import com.teradata.bigdata.tour.bean.ScenicInfo
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.redis.RedisUtil
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/2/002 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait TourFuncs {

  def getDmzRedisConnect = {
    val host = "10.221.156.232"
    val port = 8021
    new Jedis(host, port)
  }

  /**
    * 景区的配置码表
    * @param conn
    * @return       lac-cell,ScenicInfo(景区ID，景区名称，所属地市，人数警告阈值)
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

  /**
    * 从redis获取map, 景点ID<-所属地市
    * @param jedis
    * @return
    */
  def getSceneryInfoFromDmz(jedis: Jedis): mutable.HashMap[String, String] = {
    val currentKeys: util.Set[String] = jedis.keys("td:travel:realtime:current:*")
    val travelSceneryMap: mutable.HashMap[String, String] = new mutable.HashMap
    for (key <- currentKeys) {
      val fieldValues: mutable.Map[String, String] = mapAsScalaMap(jedis.hgetAll(key))
      fieldValues.foreach((fv: (String, String)) => {
        travelSceneryMap.update(fv._1, key)
      })
    }
    travelSceneryMap
  }

  def getRedisConnect(): Jedis = {
    val redisUtil = new RedisUtil
    redisUtil.getRedisConnect
  }

  def setRedisResultList(jedis: Jedis, fieldList: List[(String, String, String)]): Unit = {
    val redisUtil = new RedisUtil
    redisUtil.hSetRedisPip(jedis, fieldList)
  }

  def getTourUserSceneryId(hbaseUtil: HbaseUtil
                           , hbaseConn: Connection
                           , tableName: String
                           , keyList: Set[String]) = {
    val UserSceneryIdMap: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap

    hbaseUtil.getResultByKeyList(hbaseConn, tableName, keyList).foreach(kv => {
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

}
