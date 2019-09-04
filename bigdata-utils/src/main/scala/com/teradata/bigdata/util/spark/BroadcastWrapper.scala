package com.teradata.bigdata.util.spark

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  * @Project:
  * @Description:  广播变量封装case class
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
case class BroadcastWrapper[T: ClassTag]
(@transient private val ssc: StreamingContext,
 @transient private val _v: T) {

  @transient private var v: Broadcast[T] = ssc.sparkContext.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value



  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject.asInstanceOf[Broadcast[T]]
  }
}
