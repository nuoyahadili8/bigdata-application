package com.teradata.bigdata.intellectsms.test

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object Test {

  def main(args: Array[String]): Unit = {
    val a = "a,b,c,d,e,f"
    println(a.split(",",-1).size)


    val user = new User

    println(user.name)

    println(Test.getClass.getCanonicalName)

    println("".length)

  }

}
