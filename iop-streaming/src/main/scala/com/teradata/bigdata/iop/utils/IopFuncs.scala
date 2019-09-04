package com.teradata.bigdata.iop.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import com.teradata.bigdata.util.hbase.HbaseUtil
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Connection, Result}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait IopFuncs extends Serializable with TimeFuncs{

  //  缓存失效周期30min
  private val EXPIRY_DURATION = 30 * 60L

  //  获取13位在hbase中存储的phoneNo
  def getPartitionPhoneNo(phoneNo: String): String = {
    phoneNo.substring(phoneNo.length - 2, phoneNo.length) + phoneNo
  }

  //  获取11位正常的phoneNo
  def getUsualPhoneNo(partitionPhoneNo: String): String = {
    partitionPhoneNo.substring(2)
  }

  def getActInfo(sourceId: String): mutable.HashMap[String, (Array[String], String, Boolean)] = {
    println("getActInfo in")
    val hbaseUtil = new HbaseUtil
    val currentDate = timeMillsToDate(getCurrentTimeMillis, "yyyyMMdd")
    val resultMap: mutable.HashMap[String, (Array[String], String, Boolean)] = new mutable.HashMap[String, (Array[String], String, Boolean)]
    //营销事件配置表  b_yz_app_td_hbase:allOfEffActivity
    hbaseUtil.getAllRows("b_yz_app_td_hbase:allOfEffActivity")
      .foreach((actInfo: Result) => {
        val cells: Array[Cell] = actInfo.rawCells()
        var sourceIdSet: Set[String] = Set()
        var activityId = ""

        cells.foreach(cell => {
          val qualifier = Bytes.toString(cell.getQualifier)   //活动类型：2【换机事件】 3【开关机事件】 4【位置事件】 5【访问APP事件】
          val actInfoJsonString = Bytes.toString(cell.getValue)
          sourceIdSet = sourceIdSet.+(qualifier)
          if (qualifier.equals(sourceId)) {
            val actInfoJsonObject = JSON.parseObject(actInfoJsonString)
            val efctDate = actInfoJsonObject.getString("efctDate")    //生效日期 YYYYMMDD
            val endDate = actInfoJsonObject.getString("endDate")      //失效日期 YYYYMMDD
            //活动有效
            if (endDate > currentDate && efctDate <= currentDate) {
              activityId = Bytes.toString(cell.getRow)     // 营销活动ID
              val message = actInfoJsonObject.getString("message")   //营销内容 即短信内容
              val ResultString = actInfoJsonObject.getString("ResultString")
              val ResultSet = ResultString.split(",")

              //活动ID，（允许基站，短信内容，是否为组合事件的标记：默认为false）
              resultMap.update(activityId, (ResultSet, message, false))
            }
          }
        })

        if (sourceIdSet.size > 1 && !activityId.equals("")) {
          //          说明为组合事件，在本事件成功时不直接发出，推送到下游kafka topic处理
          val resultSet = resultMap(activityId)._1
          val message = resultMap(activityId)._2
          resultMap.update(activityId, (resultSet, message, true))
        }
      })
    resultMap
  }

  def getGroupActInfo(): mutable.HashMap[String, (String, String)] = {
    val hbaseUtil = new HbaseUtil
    val currentDate = timeMillsToDate(getCurrentTimeMillis, "yyyyMMdd")
    val resultMap: mutable.HashMap[String, (String, String)] = new mutable.HashMap[String, (String, String)]

    hbaseUtil.getAllRows("b_yz_app_td_hbase:allOfEffActivityGroup").foreach(actInfo => {
      val cells: Array[Cell] = actInfo.rawCells()
      cells.foreach(cell => {
        try {
          val actInfoJsonString = Bytes.toString(cell.getValue)
          val actInfoJsonObject = JSON.parseObject(actInfoJsonString)
          val efctDate = actInfoJsonObject.getString("efctDate")
          val endDate = actInfoJsonObject.getString("endDate")

          if (endDate > currentDate && efctDate <= currentDate) {
            val activityId = Bytes.toString(cell.getRow)
            val message = actInfoJsonObject.getString("message")
            val ResultString = actInfoJsonObject.getString("ResultString")

            resultMap.update(activityId, (ResultString, message))
          }
        }
      })
    })
    resultMap
  }

  def getUsersImei(hbaseUtil: HbaseUtil
                   , hbaseConn: Connection
                   , tableName: String
                   , keyList: Set[String]): mutable.HashMap[String, String] = {
    val resultMap: mutable.HashMap[String, String] = new mutable.HashMap
    hbaseUtil.getResultByKeyList(hbaseConn, tableName, keyList)
      .foreach(kv => {
        val phoneNo = kv._1
        val result = kv._2
        //        剔除找不到imei的用户
        if (!result.isEmpty) {
          val imei = Bytes.toString(result.getValue("0".getBytes, "imei".getBytes))
          if(imei!=null){
            resultMap.update(phoneNo, imei)
          }
        }
      })
    resultMap
  }

  def putUsersImei(hbaseUtil: HbaseUtil
                   , hbaseConn: Connection
                   , userPhoneImeiMap: mutable.HashMap[String, String]): Unit = {
    hbaseUtil.putByKeyColumnList(hbaseConn, "b_yz_app_td_hbase:UserLastStay", "0", "imei", userPhoneImeiMap.toList)
  }

  /**
    * 1.先从组合事件表查找已经营销过的用户，剔除掉
    * 2.再从单一事件表查找已经营销过的用户再剔除掉 = 未营销的用户（单一+组合）
    *
    * */
  def getUsersActListByGroupFilter(hbaseUtil: HbaseUtil
                                   , hbaseConn: Connection
                                   , tableName: String
                                   , keyList: Set[String]) = {

    var tmpSet = keyList
    val currentMonth = timeMillsToDate(getCurrentTimeMillis, "yyyyMM")
    println("getUsersActListByGroupFilter in "+timeMillsToDate(getCurrentTimeMillis,"yyyy-MM-dd HH:mm:ss"))

    //    过滤组合事件源当月已经营销用户
    hbaseUtil.getResultByKeyList(hbaseConn, "b_yz_app_td_hbase:SelectPhoneListGroup", keyList)
      .foreach(kv => {
        val phoneNo = kv._1
        val result = kv._2
        if (!result.isEmpty) {
          val cells: Array[Cell] = result.rawCells()
          cells.foreach(cell => {
            val saleMonth = Bytes.toString(CellUtil.cloneValue(cell))
            if (currentMonth.equals(saleMonth)) {
              tmpSet = tmpSet.-(phoneNo)
            }
          })
        }
      })
    getUsersActList(hbaseUtil, hbaseConn, tableName, tmpSet)
  }

  /**
    * 此方法查询单一事件在当月未营销名单 手机号，List(（活动ID）)
    * @param hbaseUtil
    * @param hbaseConn
    * @param tableName
    * @param keyList
    * @return
    */
  def getUsersActList(hbaseUtil: HbaseUtil
                      , hbaseConn: Connection
                      , tableName: String
                      , keyList: Set[String])
  : mutable.HashMap[String, List[String]] = {
    println("getUsersActList in "+timeMillsToDate(getCurrentTimeMillis,"yyyy-MM-dd HH:mm:ss"))

    val currentMonth = timeMillsToDate(getCurrentTimeMillis, "yyyyMM")

    val saleMap: mutable.HashMap[String, List[(String, String)]] = new scala.collection.mutable.HashMap

    //    过滤单一事件源当月已经营销用户
    hbaseUtil.getResultByKeyList(hbaseConn, tableName, keyList: Set[String])
      .foreach(kv => {
        val phoneNo = kv._1
        val result = kv._2
        if (!result.isEmpty) {
          val cells: Array[Cell] = result.rawCells()
          cells.foreach(cell => {
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))  //活动ID
            val saleMonth = Bytes.toString(CellUtil.cloneValue(cell))      //发送月份

            if (saleMap.contains(phoneNo)) {
              val lastList: List[(String, String)] = saleMap(phoneNo)
              saleMap.update(phoneNo, (qualifier, saleMonth) +: lastList)
            } else {
              saleMap.update(phoneNo, List((qualifier, saleMonth)))
            }
          })
        }
      })

    saleMap
      .filter(userActList => if (userActList._2.map(_._2).contains(currentMonth)) false else true)
      .map(userActList => (userActList._1, userActList._2.map(_._1)))
  }

  /**
    * 此方法查询组合事件在当月已营销名单
    * @param hbaseUtil
    * @param hbaseConn
    * @param keyList
    * @return
    */
  def getSaledGroupActUser(hbaseUtil: HbaseUtil
                           , hbaseConn: Connection
                           , keyList: Set[String]
                          ): Set[String] = {
    val currentMonth = timeMillsToDate(getCurrentTimeMillis, "yyyyMM")

    val saleMap: mutable.HashMap[String, List[(String, String)]] = new scala.collection.mutable.HashMap
    //    过滤组合事件源当月已经营销用户
    hbaseUtil.getResultByKeyList(hbaseConn, "b_yz_app_td_hbase:SelectPhoneListGroup", keyList: Set[String])
      .foreach(kv => {
        val phoneNo = kv._1
        val result = kv._2
        if (!result.isEmpty) {
          val cells: Array[Cell] = result.rawCells()
          cells.foreach(cell => {
            val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
            val saleMonth = Bytes.toString(CellUtil.cloneValue(cell))

            if (saleMap.contains(phoneNo)) {
              val lastList: List[(String, String)] = saleMap(phoneNo)
              saleMap.update(phoneNo, (qualifier, saleMonth) +: lastList)
            } else {
              saleMap.update(phoneNo, List((qualifier, saleMonth)))
            }

          })
        }
      })
    saleMap
      .filter(userActList => if (userActList._2.map(_._2).contains(currentMonth)) true else false)
      .map(_._1).toSet
  }

  def getSchoolUserStatus(hbaseUtil: HbaseUtil
                          , hbaseConn: Connection
                          , tableName: String
                          , keyList: Set[String]) = {
    val schoolUserStatus: mutable.HashMap[String, (String, Long, Long)] = new scala.collection.mutable.HashMap

    hbaseUtil.getResultByKeyList(hbaseConn, tableName, keyList).foreach(kv => {
      val phoneNo = kv._1
      val result = kv._2
      if (!result.isEmpty) {
        val eventType = Bytes.toString(result.getValue("0".getBytes(), "eventType".getBytes()))
        val startTime = Bytes.toString(result.getValue("0".getBytes(), "startTime".getBytes())).toLong
        val duration = Bytes.toString(result.getValue("0".getBytes(), "duration".getBytes())).toLong
        schoolUserStatus.update(phoneNo, (eventType, startTime, duration))
      }
    })

    schoolUserStatus
  }

  def putGroupActUserCache(hun: HbaseUtil
                           , hbaseConn: Connection
                           , sourceId: String
                           , evtEffectPutList: List[(String, String, String, String)]
                          ): Unit = {

    hun.putByKeyColumnList_IOP(hbaseConn
      , "b_yz_app_td_hbase:IopEffectEventCache"
      , "0"
      , evtEffectPutList.map(record => {
        val valueJsonObject = new JSONObject()
        valueJsonObject.put("actId", record._2)
        valueJsonObject.put("sourceId", record._3)
        (record._1, record._4, valueJsonObject.toJSONString)
      })
    )
  }

  def getGroupActUserCache(hbaseUtil: HbaseUtil
                           , hbaseConn: Connection
                           , keyList: Set[String])
  : mutable.HashMap[String, mutable.HashMap[String, String]] = {
    val resultMap: mutable.HashMap[String, mutable.HashMap[String, String]] = mutable.HashMap()
    val currentTimestamp = getCurrentTimeMillis / 1000
    hbaseUtil.getResultByKeyList(hbaseConn, "b_yz_app_td_hbase:IopEffectEventCache", keyList)
      .foreach(kv => {
        val phoneNo = kv._1
        val result = kv._2         // partitionPhoneNo, actId, SOURCE_ID, startTimeLong
        if (!result.isEmpty) {
          val cells = result.rawCells()
          cells.foreach(cell => {
            try {
              val timeStampLong = Bytes.toString(CellUtil.cloneValue(cell)).toLong
              // 半个小时以内
              if (currentTimestamp - timeStampLong < EXPIRY_DURATION) {
                val jsonObejct = JSON.parseObject(Bytes.toString(CellUtil.cloneValue(cell)))
                val actId = jsonObejct.getString("actId")
                val sourceId = jsonObejct.getString("sourceId")

                if (resultMap.contains(phoneNo)) {
                  val actSourMap = resultMap(phoneNo)
                  actSourMap.update(actId, sourceId)
                  resultMap.update(phoneNo, actSourMap)
                } else {
                  val actSourMap: mutable.HashMap[String, String] = mutable.HashMap()
                  actSourMap.update(actId, sourceId)
                  resultMap.update(phoneNo, actSourMap)
                }
              }
            }
          })
        }
      })
    resultMap
  }
}
