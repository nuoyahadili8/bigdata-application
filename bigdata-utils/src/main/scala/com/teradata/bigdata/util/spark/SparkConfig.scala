package com.teradata.bigdata.util.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.RuntimeConfig

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/26/026 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
class SparkConfig {

  def getConf = {
    new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.streaming.backpressure.enabled", "true")
      //      .set("spark.streaming.backpressure.initialRate", "800000")
      //      .set("spark.streaming.kafka.maxRatePerPartition", "160000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

      //      .set("spark.speculation", "true")
      //      .set("spark.speculation.interval", "100")
      //      .set("spark.speculation.quantile", "0.8")
      //      .set("spark.speculation.multiplier", "1.5")

      .set("spark.rdd.compress", "true")
      .set("spark.reducer.maxSizeInFlight", "144M")
      .set("spark.shuffle.io.maxRetries", "3")
      .set("spark.shuffle.io.retryWait", "30s")
      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.storage.memoryFraction", "0.5")
      /*在shuffle节点每个reduce task会启动5个fetch线程
    （可以由spark.shuffle.copier.threads配置）
    去最多spark.reducer.maxMbInFlight个(默认5)其他Excuctor中获取文件位置，
    然后去fetch它们，并且每次fetch的抓取量不会超过spark.reducer.maxMbInFlight（默认值为48MB)/5。
    这种机制我个人理解，
    第一：可以减少单个fetch连接的网络IO、
    第二：这种将fetch数据并行执行有助于抓取速度提高，减少请求数据的抓取时间总和。
    回来结合我现在的问题分析，我将spark.reducer.maxMbInFlight调小，
    从而减少了每个reduce task中的每个fetch线程的抓取数据量，
    进而减少了每个fetch连接的持续连接时间，
    降低了由于reduce task过多导致每个Excutor中存在的fetch线程太多而导致的fetch超时，另外降低内存的占用。*/
      .set("spark.reducer.maxMbInFlight", "24")

      .set("spark.task.maxFailures", "8")
      .set("spark.akka.timeout", "300")
      .set("spark.network.timeout", "300")
      .set("spark.yarn.max.executor.failures", "100")
      //设置为true时，在job结束时，保留staged文件；否则删掉这些文件。
      .set("spark.yarn.preserve.staging.files", "false")

      .set("spark.hadoop.hadoop.security.bdoc.access.id", "5754f80986433f4cb8de")
      .set("spark.hadoop.hadoop.security.bdoc.access.key", "d76256523eb684f3f814")
      //      .set("spark.hadoop.hadoop.security.proxy.user", "sunjiafeng")
      .set("queue", "root.bdoc.b_yz_app_td_yarn")
      //      .set("spark.scheduler.mode","FAIR")
      .set("spark.sql.streaming.checkpointLocation", "/user/b_yz_app_td/checkpoint/StructStreamingTest")
  }

  def setConf(conf: RuntimeConfig): Unit = {
    conf.set("spark.driver.maxResultSize", "5g")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.streaming.backpressure.enabled", "true")
    //      .set("spark.streaming.backpressure.initialRate", "800000")
    //      .set("spark.streaming.kafka.maxRatePerPartition", "160000")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    conf.set("spark.speculation", "true")
    conf.set("spark.speculation.interval", "100")
    conf.set("spark.speculation.quantile", "0.8")
    conf.set("spark.speculation.multiplier", "2")

    conf.set("spark.rdd.compress", "true")
    conf.set("spark.reducer.maxSizeInFlight", "144M")
    conf.set("spark.shuffle.io.maxRetries", "3")
    conf.set("spark.shuffle.io.retryWait", "30s")
    conf.set("spark.shuffle.file.buffer", "64k")
    conf.set("spark.shuffle.memoryFraction", "0.3")
    conf.set("spark.storage.memoryFraction", "0.5")

    //    map文件合并写磁盘
    conf.set("spark.shuffle.consolidateFiles", "true")
    //    conf.set("spark.shuffle.manager", "hash")
    /*在shuffle节点每个reduce task会启动5个fetch线程
  （可以由spark.shuffle.copier.threads配置）
  去最多spark.reducer.maxMbInFlight个(默认5)其他Excuctor中获取文件位置，
  然后去fetch它们，并且每次fetch的抓取量不会超过spark.reducer.maxMbInFlight（默认值为48MB)/5。
  这种机制我个人理解，
  第一：可以减少单个fetch连接的网络IO、
  第二：这种将fetch数据并行执行有助于抓取速度提高，减少请求数据的抓取时间总和。
  回来结合我现在的问题分析，我将spark.reducer.maxMbInFlight调小，
  从而减少了每个reduce task中的每个fetch线程的抓取数据量，
  进而减少了每个fetch连接的持续连接时间，
  降低了由于reduce task过多导致每个Excutor中存在的fetch线程太多而导致的fetch超时，另外降低内存的占用。*/
    conf.set("spark.reducer.maxMbInFlight", "24")

    conf.set("spark.task.maxFailures", "8")
    conf.set("spark.akka.timeout", "300")
    conf.set("spark.network.timeout", "300")
    conf.set("spark.yarn.max.executor.failures", "100")
    //设置为true时，在job结束时，保留staged文件；否则删掉这些文件。
    conf.set("spark.yarn.preserve.staging.files", "false")

    conf.set("spark.hadoop.hadoop.security.bdoc.access.id", "d379c08cb42c83883f3d")
    conf.set("spark.hadoop.hadoop.security.bdoc.access.key", "fe561d4d76e7321a4778")
    //      .set("spark.hadoop.hadoop.security.proxy.user", "sunjiafeng")
    conf.set("queue", "root.bdoc.b_yz_app_xy_yarn")
  }

}
