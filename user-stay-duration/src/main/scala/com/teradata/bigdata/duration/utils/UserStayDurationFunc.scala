package com.teradata.bigdata.duration.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import com.teradata.bigdata.util.hbase.HbaseUtil
import org.apache.hadoop.hbase.client.Connection
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
trait UserStayDurationFunc {

  /**
    * hbase存储内容如：00460002182210600 column=0:0, timestamp=1567561926568, value={"updTime":"1567515323569","lacCell":"18307-51573"}
    * @param hbaseUtil
    * @param conn
    * @param reImsiSet
    * @return
    */
  def getLastUserStatusMap(hbaseUtil: HbaseUtil, conn: Connection, reImsiSet: Set[String]): mutable.HashMap[String, (String, Long)] = {
    val resultMap = new mutable.HashMap[String, (String, Long)]()
    hbaseUtil.getResultByKeyList(conn, "b_yz_app_td_hbase:UserStayDuration", reImsiSet)
      .foreach(kv => {
        val imsi = kv._1
        val result = kv._2
        if (!result.isEmpty) {
          val lastStatusJsonString = Bytes.toString(kv._2.getValue("0".getBytes(), "0".getBytes()))
          val lastStatusJsonObject = JSON.parseObject(lastStatusJsonString)
          val updTime = lastStatusJsonObject.getLong("updTime")
          val lacCell = lastStatusJsonObject.getString("lacCell")
          resultMap.update(imsi, (lacCell, updTime))
        }
      })
    resultMap
  }

  /**
    * 重分区
    * @param imsi
    * @return
    */
  def partitionedImsi(imsi: String): String = {
    imsi.substring(13, 15) + imsi
  }

  /**
    * 转换为json串
    * @param lacCell
    * @param updTime
    * @return
    */
  def getUserCurrentInfoJsonString(lacCell: String, updTime: String): String = {
    val jsonObject = new JSONObject

    jsonObject.put("updTime", updTime)
    jsonObject.put("lacCell", lacCell)
    jsonObject.toJSONString
  }

  /**
    * 更新hbase表的用户最新状态信息
    * @param hun
    * @param conn
    * @param hbaseUpdateList
    */
  def putLastestUserStatusMap(hbaseUtil: HbaseUtil, conn: Connection, hbaseUpdateList: List[(String, String)]): Unit = {
    hbaseUtil.putByKeyColumnList(conn, "b_yz_app_td_hbase:UserStayDuration", "0", "0", hbaseUpdateList)
  }
}
