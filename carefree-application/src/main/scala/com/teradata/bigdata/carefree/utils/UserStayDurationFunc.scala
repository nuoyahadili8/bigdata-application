package com.teradata.bigdata.carefree.utils

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
  * @Author: <li>2019/9/11/011 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait UserStayDurationFunc {

  def getLastUserStatusMap(hun: HbaseUtil, conn: Connection, reImsiSet: Set[String])
  : mutable.HashMap[String, (String, Long)] = {
    val resultMap = new mutable.HashMap[String, (String, Long)]()
    hun.getResultByKeyList(conn, "b_yz_app_td_hbase:UserStayDuration", reImsiSet)
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

  def partitionedImsi(imsi: String): String = {
    imsi.substring(13, 15) + imsi
  }

  def getUserCurrentInfoJsonString(lacCell: String
                                   , updTime: String
                                  ): String = {
    val jsonObject = new JSONObject

    jsonObject.put("updTime", updTime)
    jsonObject.put("lacCell", lacCell)
    jsonObject.toJSONString
  }


  def putLastestUserStatusMap(hun: HbaseUtil, conn: Connection, hbaseUpdateList: List[(String, String)]): Unit = {
    hun.putByKeyColumnList(conn, "b_yz_app_td_hbase:UserStayDuration", "0", "0", hbaseUpdateList)
  }
}
