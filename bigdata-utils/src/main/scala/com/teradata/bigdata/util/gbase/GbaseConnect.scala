package com.teradata.bigdata.util.gbase

import java.sql.{Connection, DriverManager}

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/28/028 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait GbaseConnect {

  def gbaseInnerConnect(): Connection = {
    //    val CONNSTR = "jdbc:gbase://10.221.156.10/pdata?failoverEnable=true&hostList=10.221.156.9,10.221.156.8,10.221.156.7,10.221.156.6,10.221.156.5,10.221.156.4,10.221.156.3,10.221.156.2&gclusterId=gcl1"
    val CONNSTR = "jdbc:gbase://10.221.156.10/pdata?failoverEnable=true&hostList=10.221.156.9&gclusterId=gcl1"
    val USERNAME = "pdata"
    val PASSWORD = "Pdatadsjs"
    classOf[com.gbase.jdbc.Driver]
    DriverManager.getConnection(CONNSTR, USERNAME, PASSWORD)
  }

  def gbaseOutterConnect(): Connection = {
    //    val CONNSTR = "jdbc:gbase://10.221.158.10/pdata?failoverEnable=true&hostList=10.221.158.9,10.221.156.8,10.221.158.7,10.221.158.6,10.221.158.5,10.221.158.4,10.221.158.3,10.221.158.2&gclusterId=gcl1"
    val CONNSTR = "jdbc:gbase://10.221.156.10/pdata?failoverEnable=true&hostList=10.221.158.9&gclusterId=gcl1"
    val USERNAME = "pdata"
    val PASSWORD = "Pdatadsjs"
    classOf[com.gbase.jdbc.Driver]
    DriverManager.getConnection(CONNSTR, USERNAME, PASSWORD)
  }

}
