package com.teradata.bigdata.tour

import com.teradata.bigdata.tour.conf.{HbaseUtil, SparkConfig}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @Project:
  * @Description:  每日晚上11点半执行，修复旅游相关表数据
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/17/017 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object TourAnalysis {

  val classNameStr = "SparkConnectHbaseTest"
  val tourHistoryHbaseTableName = "b_yz_app_td_hbase:TourHistory"
  val sceneryCountHbaseTableName = "b_yz_app_td_hbase:SceneryCount"
  val userLastStayHbaseToHdfs = "b_yz_app_td_hbase:UserLastStay"

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .config((new SparkConfig).getConf)
      .appName(classNameStr)
      .getOrCreate()

    val hbaseUtil = new HbaseUtil
    val hbaseConfig = hbaseUtil.getConf()

    val conn = hbaseUtil.createHbaseConnection

    //    用户号码相关表预分区值
    val splitKeys: Array[Array[Byte]] = Array("0100000000000".getBytes(), "0200000000000".getBytes(), "0300000000000".getBytes(), "0400000000000".getBytes(), "0500000000000".getBytes(), "0600000000000".getBytes(), "0700000000000".getBytes(), "0800000000000".getBytes(), "0900000000000".getBytes(), "1000000000000".getBytes(), "1100000000000".getBytes(), "1200000000000".getBytes(), "1300000000000".getBytes(), "1400000000000".getBytes(), "1500000000000".getBytes(), "1600000000000".getBytes(), "1700000000000".getBytes(), "1800000000000".getBytes(), "1900000000000".getBytes(), "2000000000000".getBytes(), "2100000000000".getBytes(), "2200000000000".getBytes(), "2300000000000".getBytes(), "2400000000000".getBytes(), "2500000000000".getBytes(), "2600000000000".getBytes(), "2700000000000".getBytes(), "2800000000000".getBytes(), "2900000000000".getBytes(), "3000000000000".getBytes(), "3100000000000".getBytes(), "3200000000000".getBytes(), "3300000000000".getBytes(), "3400000000000".getBytes(), "3500000000000".getBytes(), "3600000000000".getBytes(), "3700000000000".getBytes(), "3800000000000".getBytes(), "3900000000000".getBytes(), "4000000000000".getBytes(), "4100000000000".getBytes(), "4200000000000".getBytes(), "4300000000000".getBytes(), "4400000000000".getBytes(), "4500000000000".getBytes(), "4600000000000".getBytes(), "4700000000000".getBytes(), "4800000000000".getBytes(), "4900000000000".getBytes(), "5000000000000".getBytes(), "5100000000000".getBytes(), "5200000000000".getBytes(), "5300000000000".getBytes(), "5400000000000".getBytes(), "5500000000000".getBytes(), "5600000000000".getBytes(), "5700000000000".getBytes(), "5800000000000".getBytes(), "5900000000000".getBytes(), "6000000000000".getBytes(), "6100000000000".getBytes(), "6200000000000".getBytes(), "6300000000000".getBytes(), "6400000000000".getBytes(), "6500000000000".getBytes(), "6600000000000".getBytes(), "6700000000000".getBytes(), "6800000000000".getBytes(), "6900000000000".getBytes(), "7000000000000".getBytes(), "7100000000000".getBytes(), "7200000000000".getBytes(), "7300000000000".getBytes(), "7400000000000".getBytes(), "7500000000000".getBytes(), "7600000000000".getBytes(), "7700000000000".getBytes(), "7800000000000".getBytes(), "7900000000000".getBytes(), "8000000000000".getBytes(), "8100000000000".getBytes(), "8200000000000".getBytes(), "8300000000000".getBytes(), "8400000000000".getBytes(), "8500000000000".getBytes(), "8600000000000".getBytes(), "8700000000000".getBytes(), "8800000000000".getBytes(), "8900000000000".getBytes(), "9000000000000".getBytes(), "9100000000000".getBytes(), "9200000000000".getBytes(), "9300000000000".getBytes(), "9400000000000".getBytes(), "9500000000000".getBytes(), "9600000000000".getBytes(), "9700000000000".getBytes(), "9800000000000".getBytes(), "9900000000000".getBytes())
    //    删除TourHistory
    hbaseUtil.dropTable(conn, tourHistoryHbaseTableName)
    //    重建自带预分区TourHistory表
    hbaseUtil.createTable(conn,tourHistoryHbaseTableName, splitKeys)
    //    删除SceneryCount
    hbaseUtil.dropTable(conn, sceneryCountHbaseTableName)
    //    重建SceneryCount
    hbaseUtil.createTable(conn,sceneryCountHbaseTableName)

    hbaseConfig.set(TableInputFormat.INPUT_TABLE, userLastStayHbaseToHdfs)
    val newHbaseConfig=hbaseUtil.getConf()
    newHbaseConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val sc: SparkContext = sparkSession.sparkContext
    // 读取hbase景区码表
    val selectLacCiList = hbaseUtil.getSelectLacCiAllRows
    val selectLacCiListBroadCast = sc.broadcast(selectLacCiList)

    val jobConf1 = new JobConf(newHbaseConfig, this.getClass)
    jobConf1.set(TableOutputFormat.OUTPUT_TABLE, tourHistoryHbaseTableName)

    val job1 = Job.getInstance(jobConf1)
    job1.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job1.setOutputValueClass(classOf[Result])
    job1.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val jobConf2 = new JobConf(newHbaseConfig, this.getClass)
    jobConf2.set(TableOutputFormat.OUTPUT_TABLE, sceneryCountHbaseTableName)
    val job2 = Job.getInstance(jobConf2)
    job2.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job2.setOutputValueClass(classOf[Result])
    job2.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val rs = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //   当前unix时间
    val currentUnixTime = System.currentTimeMillis()

    val userStayInfo: RDD[(String, UserStayInfo)] =rs.partitionBy(new HashPartitioner(300)).mapPartitions(partition =>{
      val rowPartition: Seq[(ImmutableBytesWritable, Result)] =partition.toList
      val ll:mutable.HashMap[String, UserStayInfo] = new scala.collection.mutable.HashMap
      rowPartition.foreach(result =>{
        val phoneNo = Bytes.toString(result._2.getRow)
        val lacCell = Bytes.toString(result._2.getValue(Bytes.toBytes("0"), Bytes.toBytes("0")))
        val timestampLacCell = result._2.listCells().get(0).getTimestamp
        //      超过24小时未更新数据视为离开景区
        if (currentUnixTime - timestampLacCell < 86400 * 1000) {
          if (selectLacCiList.contains(lacCell)) {
            val sceneryId = selectLacCiListBroadCast.value(lacCell)
            ll.update(phoneNo,UserStayInfo(sceneryId,lacCell,timestampLacCell))
          }
        }
      })
      ll.toIterator
    })

    // 将用户最后景区位置存入TourHistory
    userStayInfo.map(line =>{
      val phoneNo = line._1
      val userStayInfo = line._2
      val put = new Put(Bytes.toBytes(phoneNo))
      put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("0"), Bytes.toBytes(userStayInfo.sceneryId))
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job1.getConfiguration)

    // 聚合每个景区人数
    // 将景区最后状态存入SceneryCount
    userStayInfo.map(line =>{
      val phoneNo = line._1
      val userStayInfo = line._2
      val sceneryId = userStayInfo.sceneryId
      (phoneNo,sceneryId)
    }).groupBy(_._2).map(kv =>{
      val sceneryId = kv._1
      val sum = kv._2.toList.size
      val put = new Put(Bytes.toBytes(sceneryId))
      put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("0"), Bytes.toBytes(sum.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job2.getConfiguration)

    conn.close()
  }

}
