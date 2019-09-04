package test

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */

case class Persion(id:Int,name:String)
object Test {

  def main(args: Array[String]): Unit = {
    val list = List(Persion(1,"a"),Persion(2,"b"))

    list.iterator.foreach(println)
  }

}
