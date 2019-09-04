package com.teradata.bigdata.util.teradata

import java.sql.{Connection, DriverManager}

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/2/002 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait TeradataConnect {

  def db6800Connect(): Connection = {
    val CONNSTR = "jdbc:teradata://db6800/DBS_PORT=1025,TMODE=TERA,CHARSET=ASCII,CLIENT_CHARSET=GBK,DATABASE=dbc"
    val USERNAME = "tetl"
    val PASSWORD = "nmbi_tetl_lsc001"
    classOf[com.teradata.jdbc.TeraDriver]
    DriverManager.getConnection(CONNSTR, USERNAME, PASSWORD)
  }

  def portalConnect(): Connection = {
    val QD_CONNSTR = "jdbc:teradata://nmportal/DBS_PORT=1025,TMODE=TERA,CHARSET=ASCII,CLIENT_CHARSET=GBK,LOB_SUPPORT=off,DATABASE=dbc"
    val QD_USERNAME = "bakarc"
    val QD_PASSWORD = "bakarc"
    classOf[com.teradata.jdbc.TeraDriver]
    DriverManager.getConnection(QD_CONNSTR, QD_USERNAME, QD_PASSWORD)
  }
  def interface177Connect(): Connection ={
    val interface177="jdbc:teradata://10.221.158.177/DBS_PORT=8762,TMODE=TERA,CHARSET=ASCII,CLIENT_CHARSET=GBK,LOB_SUPPORT=off,DATABASE=dbc"
    val qd_userName = "bakarc"
    val qd_passWord = "bakarc"
    classOf[com.teradata.jdbc.TeraDriver]
    DriverManager.getConnection(interface177, qd_userName, qd_passWord)
  }
}
