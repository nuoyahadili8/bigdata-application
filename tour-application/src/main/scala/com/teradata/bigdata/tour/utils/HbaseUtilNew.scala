package com.teradata.bigdata.tour.utils

import java.util

import com.teradata.bigdata.util.tools.TimeFuncs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}


class HbaseUtilNew extends Serializable with TimeFuncs {
  var conf: Configuration = null;

  //  var conf: Configuration = null;

  def init() {
    this.synchronized {
      if (conf != null) {
        return
      }

      conf = HBaseConfiguration.create()
      //--------------------------三期新增配置
      //      conf.set("fs.defaultFS", "hdfs://nmsq")
      conf.set("hbase.rootdir", "hdfs://nmsq/apps/hbase/data")
      conf.set("hbase.zookeeper.quorum", "ss-b02-m12-a01-r5300-1,ss-b02-m12-a01-r5300-3,ss-b02-m12-a01-r5300-4,ss-b02-m12-a01-r5300-5,ss-b02-m12-a01-r5300-6")
      conf.set("hadoop.security.bdoc.access.id", "5754f80986433f4cb8de")
      conf.set("hadoop.security.bdoc.access.key", "d76256523eb684f3f814")
      conf.set("hbase.client.start.log.errors.counter", "1")
      conf.set("hbase.client.retries.number", "1")

      conf.set("zookeeper.znode.parent", "/hbase-unsecure")
      //--------------------------三期新增配置

      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.client.pause", "1000")
      conf.set("hbase.rpc.timeout", "12000")
      conf.set("hbase.client.operation.timeout", "60000")
      conf.set("hbase.client.scanner.timeout.period", "10000")
      conf.set("hbase.client.write.buffer", "6291456")
      conf.set("hbase.zookeeper.property.maxClientCnxns", "1000")
      conf.set("hbase.regionserver.handler.count", "30000")
      //      conf.set("hbase.client.scanner.caching", "300000")
      //      conf.set("hbase.client.keyvalue.maxsize", "52428800")


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


  def putByKeyColumnList(conn: Connection
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


  def getResultByKeyList(conn: Connection, tableName: String, keyList: Set[String])
  : mutable.HashMap[String, Result] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, Result] = new scala.collection.mutable.HashMap
    try {

      val muList = new ListBuffer[Get]
      keyList.foreach(key => {
        val rowKey = key
        muList.add(
          new Get(rowKey.getBytes).addFamily("0".getBytes())
        )
      })

      println("muList======" + muList.size)
      val getList = table.get(muList)
      getList.foreach(result => {
        if (!result.isEmpty) {
          val key = Bytes.toString(result.getRow)
          results.update(key, result)
        }
      })

    } finally {
      if (table != null) table.close()
    }

    results
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

  def getAllRows(tableName: String, family: String, qualifier: String): Map[String, String] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner(new Scan())
    val it: util.Iterator[Result] = results.iterator()
    val lstBuffer = ListBuffer[(String, String)]()
    while (it.hasNext) {
      val next: Result = it.next()
      lstBuffer.+=((new String(next.getRow), new String(next.getValue(family.getBytes, qualifier.getBytes))))
    }
    lstBuffer.toMap
  }

  def getAllRows_TOUR(tableName: String, family: String): Map[String, (String, String, String, String)] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner(new Scan())
    val it: util.Iterator[Result] = results.iterator()
    val lstBuffer = ListBuffer[(String, (String, String, String, String))]()
    while (it.hasNext) {
      val next: Result = it.next()
      val SCENERY_ID = new String(next.getValue(family.getBytes, "SCENERY_ID".getBytes))
      val SCENERY_NAME = new String(next.getValue(family.getBytes, "SCENERY_NAME".getBytes))
      val CITY_ID = new String(next.getValue(family.getBytes, "CITY_ID".getBytes))
      val ALARM_VALUE = new String(next.getValue(family.getBytes, "ALARM_VALUE".getBytes))
      lstBuffer.+=((new String(next.getRow), (SCENERY_ID, SCENERY_NAME, CITY_ID, ALARM_VALUE)))
    }
    lstBuffer.toMap
  }

  def getAllRows_TOUR2(tableName: String, family: String): mutable.HashMap[String, Int] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner(new Scan())
    val it: util.Iterator[Result] = results.iterator()
    val resultMap: mutable.HashMap[String, Int] = new scala.collection.mutable.HashMap
    while (it.hasNext) {
      val next: Result = it.next()
      val sum = new String(next.getValue(family.getBytes, "sum".getBytes))
      resultMap.update(new String(next.getRow), sum.toInt)
    }
    resultMap
  }

  def getAllRows(tableName: String): List[Result] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val rs: ResultScanner = table.getScanner(new Scan())
    val rsList: List[Result] = rs.toList

    rs.close()
    table.close()
    rsList

  }

  def deleteRows(tablename: String, rowkeys: List[String]): Unit = {
    this.init()
    val table = new HTable(conf, tablename)
    val muList = new ListBuffer[Delete]
    rowkeys.foreach((key: String) => {
      muList.add(new Delete(key.getBytes()))
    }
    )

    table.delete(muList)
    table.close();
  }

  def getTourHistory(conn: Connection, tableName: String): mutable.HashMap[String, Int] = {
    val scan = new Scan()
    val filter =
      new SingleColumnValueFilter(Bytes.toBytes("0")
        , Bytes.toBytes("0")
        , CompareOp.NOT_EQUAL
        , Bytes.toBytes("X"));

    scan.setFilter(filter)
    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner(scan)
    val it: util.Iterator[Result] = results.iterator()
    val resultMap: mutable.HashMap[String, Int] = new mutable.HashMap()
    while (it.hasNext) {
      val res = it.next()
      val sceneryId = Bytes.toString(res.getValue("0".getBytes(), "0".getBytes()))
      if (resultMap.contains(sceneryId)) {
        val sceneryCount = resultMap(sceneryId)
        resultMap.update(sceneryId, sceneryCount + 1)
      } else {
        resultMap.update(sceneryId, 1)
      }
    }
    resultMap
  }
}





