package com.teradata.bigdata.util.hbase

import java.util

import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

/**
  * @Project:
  * @Description: hbase工具类
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
class HbaseUtil extends Serializable with TimeFuncs{

  var conf: Configuration = null

  def init() {
    this.synchronized {
      if (conf != null) {
        return
      }

      conf = HBaseConfiguration.create()
      //conf.set("hadoop.security.bdoc.access.id", "d690843cba5facbce853")
      //conf.set("hadoop.security.bdoc.access.key", "2e6212edd46b91aa3055baa965b01f6f8cf9f482")
      //      conf.addResource("hbase-site.xml")
      //      conf.addResource("core-site.xml")
      //--------------------------三期新增配置
      //      conf.set("fs.defaultFS", "hdfs://nmsq")
      conf.set("hbase.rootdir", "hdfs://nmsq/apps/hbase/data")
      conf.set("hbase.zookeeper.quorum", "ss-b02-m12-a01-r5300-1,ss-b02-m12-a01-r5300-3,ss-b02-m12-a01-r5300-4,ss-b02-m12-a01-r5300-5,ss-b02-m12-a01-r5300-6")
      //            conf.set("hbase.zookeeper.quorum", "SS-B02-M12-A01-R5300-1")
      //      conf.set("ha.zookeeper.quorum", "ss-b02-m12-a01-r5300-1:2181,ss-b02-m12-a01-r5300-3:2181,ss-b02-m12-a01-r5300-4:2181,ss-b02-m12-a01-r5300-5:2181,ss-b02-m12-a01-r5300-6:2181")
      conf.set("hadoop.security.bdoc.access.id", "5754f80986433f4cb8de")
      conf.set("hadoop.security.bdoc.access.key", "d76256523eb684f3f814")
      conf.set("hbase.client.start.log.errors.counter", "1")
      conf.set("hbase.client.retries.number", "1")

      conf.set("zookeeper.znode.parent", "/hbase-unsecure")
      //--------------------------三期新增配置

      //conf.set("fs.defaultFS", "hdfs://nmdsj133nds")
      //conf.set("hbase.zookeeper.quorum", "ss-b02-m20-d11-r730-1,ss-b02-m20-d12-r730-1,ss-b02-m20-d12-r730-2,ss-b02-m20-d13-r730-1,ss-b02-m20-d13-r730-2")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      //conf.set("zookeeper.znode.parent", "/IOP_TD")

      //      pause时长，在hbase发生get或其他操作fail掉的时候进行pause的时间长度
      //      所以如果重试10次,hbase.client.pause=50ms，则每次重试等待时间为{50，100，150，250，500，1000，2000，5000，5000，5000}。
      conf.set("hbase.client.pause", "1000")
      //      "hbase.client.retries.number"默认35次
      //      conf.set("hbase.client.retries.number", "100")
      //      Hbase client发起远程调用时的超时时限，使用ping来确认连接，但是最终会抛出一个TimeoutException，默认值是60000；
      conf.set("hbase.rpc.timeout", "12000")
      conf.set("hbase.client.operation.timeout", "60000")
      conf.set("hbase.client.scanner.timeout.period", "10000")
      conf.set("hbase.client.write.buffer", "6291456")
      conf.set("hbase.zookeeper.property.maxClientCnxns", "1000")
      conf.set("hbase.regionserver.handler.count", "30000")
      //conf.set("hbase.client.scanner.caching", "3000")
    }
  }

  def getConf() = {
    conf = HBaseConfiguration.create()
    //conf.set("hadoop.security.bdoc.access.id", "d690843cba5facbce853")
    //conf.set("hadoop.security.bdoc.access.key", "2e6212edd46b91aa3055baa965b01f6f8cf9f482")
    //      conf.addResource("hbase-site.xml")
    //      conf.addResource("core-site.xml")
    //--------------------------三期新增配置
    //      conf.set("fs.defaultFS", "hdfs://nmsq")
    conf.set("hbase.rootdir", "hdfs://nmsq/apps/hbase/data")
    conf.set("hbase.zookeeper.quorum", "ss-b02-m12-a01-r5300-1,ss-b02-m12-a01-r5300-3,ss-b02-m12-a01-r5300-4,ss-b02-m12-a01-r5300-5,ss-b02-m12-a01-r5300-6")
    //            conf.set("hbase.zookeeper.quorum", "SS-B02-M12-A01-R5300-1")
    //      conf.set("ha.zookeeper.quorum", "ss-b02-m12-a01-r5300-1:2181,ss-b02-m12-a01-r5300-3:2181,ss-b02-m12-a01-r5300-4:2181,ss-b02-m12-a01-r5300-5:2181,ss-b02-m12-a01-r5300-6:2181")
    conf.set("hadoop.security.bdoc.access.id", "5754f80986433f4cb8de")
    conf.set("hadoop.security.bdoc.access.key", "d76256523eb684f3f814")
    conf.set("hbase.client.start.log.errors.counter", "1")
    conf.set("hbase.client.retries.number", "1")

    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //--------------------------三期新增配置

    //conf.set("fs.defaultFS", "hdfs://nmdsj133nds")
    //conf.set("hbase.zookeeper.quorum", "ss-b02-m20-d11-r730-1,ss-b02-m20-d12-r730-1,ss-b02-m20-d12-r730-2,ss-b02-m20-d13-r730-1,ss-b02-m20-d13-r730-2")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("zookeeper.znode.parent", "/IOP_TD")

    //      pause时长，在hbase发生get或其他操作fail掉的时候进行pause的时间长度
    //      所以如果重试10次,hbase.client.pause=50ms，则每次重试等待时间为{50，100，150，250，500，1000，2000，5000，5000，5000}。
    conf.set("hbase.client.pause", "1000")
    //      "hbase.client.retries.number"默认35次
    //      conf.set("hbase.client.retries.number", "100")
    //      Hbase client发起远程调用时的超时时限，使用ping来确认连接，但是最终会抛出一个TimeoutException，默认值是60000；
    //      conf.set("hbase.rpc.timeout", "12000")
    conf.set("hbase.client.operation.timeout", "60000")
    conf.set("hbase.client.scanner.timeout.period", "10000")
    conf.set("hbase.client.write.buffer", "6291456")
    conf.set("hbase.zookeeper.property.maxClientCnxns", "1000")
    conf.set("hbase.regionserver.handler.count", "300")

    conf
  }

  def createHbaseConnection: Connection = {
    this.init()
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  /**
    * 根据表的RowKey、列族、列，获取结果，放到Result中
    *
    * @param conn      Hbase连接
    * @param tableName 表名称
    * @param rowKey    RowKey
    * @param family    列族名
    * @param qualifier 列名
    * @return
    */
  def getValuesByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifier: String): Result = {
    var result: Result = null
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try {
      val g = new Get(Bytes.toBytes(rowKey))
      g.addColumn(family.getBytes(), qualifier.getBytes())
      result = table.get(g)
    } finally {
      if (table != null) table.close()
    }
    result
  }

  def getValuesByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String): Result = {
    var result: Result = null
    var table: Table = null
    try {
      table = conn.getTable(TableName.valueOf(tableName))
      val g = new Get(Bytes.toBytes(rowKey))
      g.addFamily(family.getBytes())
      result = table.get(g)
    } catch {
      case en: NullPointerException => println("---------NullPointerException---------:" + tableName + " " + rowKey)
    } finally {
      if (table != null) table.close()
    }
    result
  }


  /**
    * 根据表名称、RowKey、列族名、列名、值，写入Hbase
    *
    * @param conn      HBase连接
    * @param tableName 表名
    * @param rowKey    RowKey
    * @param family    列族名
    * @param qualifier 列名
    * @param value     Value
    */
  def putByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifier: String, value: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      //准备插入一条 key 为 rowKey 的数据
      val p = new Put(rowKey.getBytes).setWriteToWAL(false)
      //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      p.addColumn(family.getBytes, qualifier.getBytes, value.getBytes)
      //提交
      table.put(p)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_MAS(conn: Connection, tableName: String, resultList: List[(String, (String, Long, Long))]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rphone_no = res._1
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        val eventType = res._2._1
        val startTime = res._2._2
        val duration = res._2._3

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "eventType".getBytes(), eventType.getBytes())
            .addColumn("0".getBytes(), "startTime".getBytes(), startTime.toString.getBytes())
            .addColumn("0".getBytes(), "duration".getBytes(), duration.toString.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_USER(conn: Connection, tableName: String, resultList: List[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rphone_no = res._1
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        val flag = res._2

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "flag".getBytes(), flag.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_Leader(conn: Connection, tableName: String, resultList: List[(String, (String, String))]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val phone_no = res._1
        val (qualifier, value) = res._2

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), qualifier.getBytes(), value.getBytes())

        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putResultByKeyList_IOP(conn: Connection, tableName: String, keyList: List[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val currentMonth = timeMillsToDate(getCurrentTimeMillis, "yyyyMM")
    try {
      val muList = new ListBuffer[Put]
      keyList.foreach(res => {
        val rowKey = res._1
        val qualifier = res._2

        muList.add(
          new Put(rowKey.getBytes)
            .setWriteToWAL(false)
            .addColumn("0".getBytes(), qualifier.getBytes(), currentMonth.getBytes())
        )
      })
      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_IOP(conn: Connection
                         , tableName: String
                         , family: String
                         , resultList: List[(String, String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rowKey = res._1
        val qualifier = res._2
        val value = res._3

        muList.add(
          new Put(rowKey.getBytes)
            .setWriteToWAL(false)
            .addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }


  def putByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifierValueList: Iterator[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      //准备插入一条 key 为 rowKey 的数据
      val p = new Put(rowKey.getBytes).setWriteToWAL(false)
      //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      while (qualifierValueList.hasNext) {
        val qualifierValue = qualifierValueList.next()
        val qualifier = qualifierValue._1
        val value = qualifierValue._2
        p.addColumn(family.getBytes, qualifier.getBytes, value.getBytes)
      }
      //提交
      table.put(p)
    } finally {
      if (table != null) table.close()
    }
  }

  def getAllRows(tableName: String, family: String, qualifier: String): Map[String, String] = {
    val conn: Connection = this.createHbaseConnection
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val results: ResultScanner = table.getScanner(new Scan())
    val it: util.Iterator[Result] = results.iterator()
    val lstBuffer = ListBuffer[(String, String)]()
    while (it.hasNext) {
      val next: Result = it.next()
      lstBuffer.+=((new String(next.getRow), new String(next.getValue(family.getBytes, qualifier.getBytes))))
    }
    conn.close()
    lstBuffer.toMap
  }


  def getAllRows(tableName: String): List[Result] = {
    val conn: Connection = this.createHbaseConnection
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val rs: ResultScanner = table.getScanner(new Scan())
    val rsList: List[Result] = rs.toList

    rs.close()
    table.close()
    conn.close()
    rsList
  }

  def getResultByKeyList(conn: Connection, tableName: String, keyList: Set[String]): mutable.HashMap[String, Result] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, Result] = new scala.collection.mutable.HashMap
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(key => {
        val rowKey = key
        muList.add(
          new Get(rowKey.getBytes)
        )
      })

      table.get(muList)
        .foreach(result => {
          val key = Bytes.toString(result.getRow)
          results.update(key, result)
        })

    } finally {
      if (table != null) table.close()
    }

    results
  }

  /**
    * 返回结果：手机号，（需求ID，进入需求时间，时长）
    * @param conn
    * @param tableName
    * @param keyList
    * @return
    */
  def getResultByKeyList_MAS(conn: Connection, tableName: String, keyList: List[String]): mutable.HashMap[String, (String, Long, Long)] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, (String, Long, Long)] = new scala.collection.mutable.HashMap
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(rphone_no => {
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        muList.add(
          new Get(phone_no.getBytes).addFamily("0".getBytes())
        )
      })

      val getList = table.get(muList)
      getList.foreach(result => {
        if (!result.isEmpty) {
          val prePhoneNo = Bytes.toString(result.getRow)
          val phoneNo = prePhoneNo.substring(2)
          val eventType = Bytes.toString(result.getValue("0".getBytes(), "eventType".getBytes()))
          // 进入这个需求的时间
          val startTime = Bytes.toString(result.getValue("0".getBytes(), "startTime".getBytes())).toLong
          val duration = Bytes.toString(result.getValue("0".getBytes(), "duration".getBytes())).toLong
          results.update(phoneNo, (eventType, startTime, duration))
        }
      })

    } finally {
      if (table != null) table.close()
    }
    results
  }


  def getResultByKeyList_Leader(tableName: String, keyList: List[String]): mutable.HashMap[String, (String, String)] = {
    val resultsMap: mutable.HashMap[String, (String, String)] = new scala.collection.mutable.HashMap
    val keySet = keyList.toSet
    val results = getAllRows(tableName)
    results.foreach(result => {
      val cells = result.rawCells()
      cells.foreach(cell => {
        val phoneNo = Bytes.toString(cell.getRowArray)
        val eventType = Bytes.toString(cell.getQualifierArray)
        val leaveFlag = Bytes.toString(cell.getValueArray)
        if (keySet.contains(phoneNo)) {
          resultsMap.update(phoneNo, (eventType, leaveFlag))
        }
      })
    })
    resultsMap
  }

  def getResultByKeyList_Leader(conn: Connection, tableName: String, family: String, keyList: List[String]): mutable.HashMap[String, (String, String)] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, (String, String)] = new scala.collection.mutable.HashMap
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(rphone_no => {
        muList.add(
          new Get(rphone_no.getBytes).addFamily("0".getBytes())
        )
      })
      val result = table.get(muList)
      result.foreach(r => {
        val cells = r.rawCells()
        cells.foreach(cell => {
          val phoneNo = Bytes.toString(cell.getRowArray)
          val eventType = Bytes.toString(cell.getQualifierArray)
          val leaveFlag = Bytes.toString(cell.getValueArray)
          results.update(phoneNo, (eventType, leaveFlag))
        })
      })
    } finally {
      if (table != null) table.close()
    }

    results

  }


  /**
    * 删除操作
    * @param conn
    * @param tableName
    * @param rowkeys
    */
  def deleteRows(conn: Connection, tableName: String, rowkeys: List[String]): Unit = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val muList = new ListBuffer[Delete]
    rowkeys.foreach((rphone_no: String) => {
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        muList.add(new Delete(phone_no.getBytes()))
      }
    )
    table.delete(muList)
    table.close()
  }

  /**
    * hbase批量插入
    * @param conn
    * @param tableName
    * @param family
    * @param qualifier
    * @param resultList
    */
  def putByKeyColumnList(conn: Connection
                         , tableName: String
                         , family: String
                         , qualifier: String
                         , resultList: List[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rowKey = res._1
        val value = res._2

        muList.add(
          new Put(rowKey.getBytes)
            .setWriteToWAL(false)
            .addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  /**
    * hbase批量插入
    * @param conn
    * @param tableName
    * @param resultList
    */
  def putByKeyColumnListInt(conn: Connection, tableName: String, resultList: List[(String, Int)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val scenery = res._1
        val count = res._2

        muList.add(
          new Put(scenery.getBytes)
            .setWriteToWAL(false)
            .addColumn("0".getBytes(), "0".getBytes(), count.toString.getBytes())

        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putResultByKeyList_SCHOOL(conn: Connection, tableName: String, resultList: List[(String, (String, Long, Long))]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val phone_no = res._1
        val eventType = res._2._1
        val startTime = res._2._2
        val duration = res._2._3

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "eventType".getBytes(), eventType.getBytes())
            .addColumn("0".getBytes(), "startTime".getBytes(), startTime.toString.getBytes())
            .addColumn("0".getBytes(), "duration".getBytes(), duration.toString.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

}
