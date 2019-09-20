package com.teradata.bigdata.intellectsms.test

import java.util.Calendar

import com.teradata.bigdata.intellectsms.IntelligentSMSApplicationAuto.judgeConditionsAndSend
import com.teradata.bigdata.intellectsms.YunmasFunc
import com.teradata.bigdata.intellectsms.users.YunmasActInfo
import com.teradata.bigdata.util.tools.TimeFuncs
import scala.collection.Map
import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object Test extends TimeFuncs with YunmasFunc{

  def main(args: Array[String]): Unit = {
    getYunmasActs.foreach(println)

//    val userCurrentInfo = (("2019-09-19 19:59:35",1568894375L),"15148087303","0475","3","471","0471","18259","81162518")
//
//    val yunmasActInfo = YunmasActInfo(Set("0475"),Set(),Set("1","2","3"),4*60)
//
//    val yunmasUserLastStatus = mutable.HashMap("15148087303"->("9",1568894075L,0L))
//
//    judgeConditionsAndSend(null
//      , "a"
//      , userCurrentInfo
//      , "9"
//      , yunmasActInfo
//      , yunmasUserLastStatus)
//
//    println(yunmasUserLastStatus("15148087303"))

//    val a = "a,b,c,d,e,f"
//    println(a.split(",",-1).size)
//
//
//    val user = new User
//
//    println(user.name)
//
//    println(Test.getClass.getCanonicalName)
//
//    println("".length)
//
//    println(OffsetRequest.LargestTimeString)

  }

}
