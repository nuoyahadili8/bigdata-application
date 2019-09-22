package com.teradata.bigdata.intellectsms.test


import com.teradata.bigdata.intellectsms.YunmasFunc
import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.util.control.Breaks._

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
  val log: Logger = org.apache.log4j.LogManager.getLogger("Test")

  def main(args: Array[String]): Unit = {
//    getYunmasActs.foreach(println)


    val mm = mutable.Map(1->("a",1),2->("b",2),3->("c",3))

    val list = List(1,2,3)

    def juage(x:Int): Boolean ={
      if(x == 2){
        true
      }else{
        false
      }
    }


    list.foreach(x =>{
      breakable{
        mm.foreach(y =>{
          println(x + "###" + y._2)
          val isSuccess = {
            if (x ==2 && y._2._1.equals("a")){
              false
            }else{
              true
            }
          }
          if (!isSuccess){
            break
          }
        })
      }
    })

    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    breakable{
      mm.foreach(y =>{
        list.foreach(x=>{
          println(x + "###" + y._2)
          val isSuccess = {
            if (x ==2 && y._2._1.equals("a")){
              println(x + ">>>" + y._2)
              true
            }else{
              false
            }
          }
          if (isSuccess){
            break
          }
        })
      })
    }



//    println(mm)

//    val userCurrentInfo = (("2019-09-19 19:59:35",1568894375L),"15148087303","0475","3","471","0471","18259","81162518")
//
//    val yunmasActInfo = YunmasActInfo(Set("0475"),Set(),Set("1","2","3"),4*60)
//
//    val yunmasUserLastStatus = mutable.HashMap("15148087303"->("9",1568894075L,0L))
//
//    judgeConditionsAndSendTest(null
//      , "a"
//      , userCurrentInfo
//      , "9"
//      , yunmasActInfo
//      , yunmasUserLastStatus
//      , log)
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
