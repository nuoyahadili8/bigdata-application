package com.teradata.bigdata.util.redis

import redis.clients.jedis.{Jedis, Response}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/2/002 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
class RedisUtil extends Serializable {


  /**
    *
    * @return 返回jedis对象
    */
  def getRedisConnect: Jedis = {
    val REDIS_HOST = "10.221.156.230"
    //    val REDIS_HOST = "10.221.158.175"
    val REDIS_PORT = 6379
    new Jedis(REDIS_HOST, REDIS_PORT, 20000, 20000)
  }

  /**
    * 获取Key信息
    * @param jedis
    * @param tableName
    * @param fieldList
    * @return  返回：景点ID，JsonString()
    */
  def hGetRedisPip(jedis: Jedis, tableName: String, fieldList: List[String]): mutable.HashMap[String, Object] = {
    val pip = jedis.pipelined()
    val map: mutable.HashMap[String, Response[String]] = new mutable.HashMap()
    fieldList.foreach(field => {
      map.update(field, pip.hget(tableName, field))
    })
    pip.sync()
    pip.close()
    map.map(kv => {
      (kv._1, kv._2.get())
    })
  }

  /**
    *
    * @param jedis
    * @param fieldList
    */
  def hSetRedisPip(jedis: Jedis, fieldList: List[(String, String, String)]): Unit = {
    val pip = jedis.pipelined()
    fieldList.foreach(tkv => {
      val tableName = tkv._1
      val field = tkv._2
      val value = tkv._3
      pip.hset(tableName, field, value)
    })
    pip.sync()
    pip.close()
  }

  def hSetRedisPip(jedis: Jedis, tableName: String, fieldList: List[(String, String)]) = {
    val pip = jedis.pipelined()
    fieldList.foreach(fieldValue => {
      val field = fieldValue._1
      val value = fieldValue._2
      pip.hset(tableName, field, value)
    })
    pip.sync()
    pip.close()
  }

  def hDelRedisPip(jedis: Jedis, tableName: String, fieldList: List[String]) = {
    val pip = jedis.pipelined()
    fieldList.foreach(field => {
      pip.hdel(tableName, field)
    })
    pip.sync()
    pip.close()
  }

  def hGetAllRedisPip(jedis: Jedis, tableName: String) = {
    val pip = jedis.pipelined()
    val tableResults = pip.hgetAll(tableName)
    pip.sync()
    pip.close()
    tableResults.get().toSet
  }

  def publishMessageRedisPip(jedis: Jedis, messages: List[(String, String)]): Unit = {
    val pip = jedis.pipelined()
    messages.foreach(messInfo => {
      val channelName = messInfo._1
      val mess = messInfo._2
      pip.publish(channelName, mess)
    })
    pip.sync()
    pip.close()
  }

  def publistMessage(jedis: Jedis, channelName: String, message: String): Unit = {
    jedis.publish(channelName, message)
  }

}
