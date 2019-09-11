package com.teradata.bigdata.carefree.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.RuntimeConfig

class SparkConfig {
  def getConf = {
    new SparkConf()
      //
      .set("spark.driver.maxResultSize", "5g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .set("spark.streaming.backpressure.enabled", "true")
      //      .set("spark.streaming.backpressure.initialRate", "800000")
      //      .set("spark.streaming.kafka.maxRatePerPartition", "160000")
      // 不fullgc
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

      /**
        * 当有hbase的时候 或者 数据库连接的时候 不使用。 当任务task完成80%的时候还有任务未完成，就启动相同类似的任务副本，谁先执行完用谁的结果
        * 反正就是有数据库连接 kafka 等有存在事务的时候  不使用。
        */
      .set("spark.speculation", "true")
      .set("spark.speculation.interval", "100")
      .set("spark.speculation.quantile", "0.8")
      .set("spark.speculation.multiplier", "2")
//    个参数决定了RDD Cache的过程中，RDD数据在序列化之后是否进一步进行压缩再储存到内存或磁盘上。当然是为了进一步减小Cache数据的尺寸,如果在磁盘IO的确成为问题或者GC问题真的没有其它更好的解决办法的时候，可以考虑启用RDD压缩。
      .set("spark.rdd.compress", "true")
      //在Shuffle的时候，Reducer端获取数据会有一个指定大小的缓存空间，如果内存足够大的情况下，可以适当的增大缓存空间，否则会spill到磁盘上影响效率。
      .set("spark.reducer.maxSizeInFlight", "144M")

      /**
        * 参数说明：shuffle read task从shuffle write task所在节点拉取属于自己的数据时，如果因为网络异常导致拉取失败，是会自动进行重试的。该参数就代表了可以重试的最大次数。如果在指定次数之内拉取还是没有成功，就可能会导致作业执行失败。
        * 调优建议：对于那些包含了特别耗时的shuffle操作的作业，建议增加重试最大次数（比如60次），以避免由于JVM的full gc或者网络不稳定等因素导致的数据拉取失败。在实践中发现，对于针对超大数据量（数十亿~上百亿）的shuffle过程，调节该参数可以大幅度提升稳定性。
        */
      .set("spark.shuffle.io.maxRetries", "3")

      /**
        * 具体解释同上，该参数代表了每次重试拉取数据的等待间隔，默认是5s。
        * 调优建议：建议加大间隔时长（比如60s），以增加shuffle操作的稳定性。
        */
      .set("spark.shuffle.io.retryWait", "30s")

      /**
        * 参数说明：该参数用于设置shuffle write task的BufferedOutputStream的buffer缓冲大小。将数据写到磁盘文件之前，会先写入buffer缓冲中，待缓冲写满之后，才会溢写到磁盘。
        * 调优建议：如果作业可用的内存资源较为充足的话，可以适当增加这个参数的大小（比如64k），从而减少shuffle write过程中溢写磁盘文件的次数，也就可以减少磁盘IO次数，进而提升性能。在实践中发现，合理调节该参数，性能会有1%~5%的提升。
        */
      .set("spark.shuffle.file.buffer", "64k")

      .set("spark.shuffle.memoryFraction", "0.3")
      .set("spark.storage.memoryFraction", "0.5")

      /**
        * 参数说明：如果使用HashShuffleManager，该参数有效。如果设置为true，那么就会开启consolidate机制，会大幅度合并shuffle write的输出文件，对于shuffle read task数量特别多的情况下，这种方法可以极大地减少磁盘IO开销，提升性能。
        * 调优建议：如果的确不需要SortShuffleManager的排序机制，那么除了使用bypass机制，还可以尝试将spark.shffle.manager参数手动指定为hash，使用HashShuffleManager，同时开启consolidate机制。在实践中尝试过，发现其性能比开启了bypass机制的SortShuffleManager要高出10%~30%。
        */
      //    map文件合并写磁盘,防止碎小文件，减少Io
      .set("spark.shuffle.consolidateFiles", "true")
//      .set("spark.shuffle.manager", "hash")
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

      .set("spark.hadoop.hadoop.security.bdoc.access.id", "d379c08cb42c83883f3d")
      .set("spark.hadoop.hadoop.security.bdoc.access.key", "fe561d4d76e7321a4778")
      //      .set("spark.hadoop.hadoop.security.proxy.user", "sunjiafeng")
      .set("queue", "root.bdoc.b_yz_app_xy_yarn")
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
