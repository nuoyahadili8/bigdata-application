package com.teradata.bigdata.util.tools

import java.io.{FileInputStream, UnsupportedEncodingException}
import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.commons.lang.SystemUtils

import scala.util.Try
import scala.util.control.ControlThrowable

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
class Utils extends LogUtil {

  val properties = new Properties()

  //val defaultPluginProps = "sourceStreaming.properties"
  //val systemDefaultDir = "./"
  //val systemDefaultPath: String = getIopClassLoader.getResource(systemDefaultDir).getPath
  //println("NOTED! System Default Path is:" + systemDefaultPath)

  val defaultPluginProps = "sourceStreaming.properties"
  def ini(): Unit = {
    try {
      //文件要放到resource文件夹下
      //			val path = Thread.currentThread().getContextClassLoader.getResource(defaultPluginProps).getPath
      //			val b =  this.getClass.getClassLoader.getResource()
      val path: String = getIopClassLoader.getResource(defaultPluginProps).getPath
      //println("path:" + path)
      properties.load(new FileInputStream(path))
    } catch {
      case e: Exception => log.debug(e.getMessage)
    }
  }

  this.ini
  /**
    * Get the ClassLoader which loaded OpenApi
    *
    * @return
    */
  def getIopClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * 获得当前线程级别的Context ClassLoader，如果不存在则载入IOP 的全局ClassLoader
    *
    * @return
    */
  def getContextOrIopClassLoader: ClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getIopClassLoader)

  /**
    * 检查class是否在当前线程中
    *
    * @param clazz 类名
    * @return
    */
  def classIsLoadable(clazz: String): Boolean = {
    Try {
      Class.forName(clazz, false, getContextOrIopClassLoader)
    }.isSuccess
  }

  def classForName(className: String): Class[_] = Class.forName(className, true, getContextOrIopClassLoader)

  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => UncaughtExceptionHandler.uncaughtException(t)
    }
  }

  private var customHostname: Option[String] = sys.env.get("OPEANAPI_LOCAL_HOSTNAME")

  /**
    * Get the local machine's hostname.
    */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  /**
    * Whether the underlying operating system is Windows.
    */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }


  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)


  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("OPENAPI_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      //windows下返回 "Evan-PC/192.168.142.1"
      //Linux下返回 "127.0.0.1"
      println("------1-----")

      logDebug("进入findLocalIpAddress.....")

      val address: InetAddress = InetAddress.getLocalHost //Returns the address of the local host

      if (address.isLoopbackAddress) {
        //
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
            !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      logDebug("本地 address" + address.getHostAddress)
      address.getHostAddress //返回具体的IP地址
    }
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  /**
    * 识别两个Map的Keys是否一致，一致返回true,不一致返回false
    *
    * @param a 识别对象A
    * @param b 识别对象B
    * @tparam K 键类型
    * @tparam V 值类型
    * @return
    */
  def mapKeySetEquels[K <: String, V](a: Map[K, V], b: Map[K, V]): Boolean = {
    var isMapKeySetEquels = false
    if (((a.keySet | b.keySet) &~ a.keySet).isEmpty && ((a.keySet | b.keySet) &~ b.keySet).isEmpty)
      isMapKeySetEquels = true
    isMapKeySetEquels
  }

  /**
    * 比较两个List是否一致，A类型必须是可比较的
    */
  def compareList[A](a: List[A], b: List[A]): Boolean = {
    if ((a diff b).isEmpty && (b diff a).isEmpty)
      true
    else false
  }


  /**
    * 比较两个Set是否一致，A类型必须是可比较的
    */
  def compareSet[A](a: Set[A], b: Set[A]): Boolean = {
    if ((a &~ b).isEmpty && (b &~ a).isEmpty)
      true
    else false
  }


  /**
    * 功能：将一个Map(K,V)折叠为Map(V,Set(K))
    * 参考：http://www.cnblogs.com/tugeler/p/5134862.html
    * 详细描述：1.搜索Map[V, Set[K]的是时候，我们发现有三个地方写了这个东西，其实这三个地方有着千丝万缕的关系：
    * 第一处代表最终我们要生成的结果，
    * 第二处代表初始值
    * 第三处代表循环折叠体：resultMap，其实就是我们最终要返回的结果，它其实在函数体内是var类型的
    *          2./: 折叠函数，是颗粒化的函数，
    * 第一组参数(A /: B)，A是结果值的初始值，B是原始“集合”值
    * 第二组参数是一个函数 func{(R,Z) => op}，该函数又有两个重要的参数R,Z
    * R是最终折叠计算的结果变量，其类型就是整个折叠函数的结果类型
    * Z是B.foreach的元素
    *          3.开始运算的时候，首先会将A赋值给R，然后从B中依次循环出来的值给Z；
    * 然后在op函数体中一定是将计算结果迭代式的赋值给R。数学函数中一般是R=op(R,Z)，而集合操作则是R+=op(R,Z)
    *
    * @param origMap 原始Map
    * @return R，本函数返回resultMap的最终结果
    */
  def reverseMap[K, V](origMap: Map[K, V]): Map[V, Set[K]] = (Map[V, Set[K]]() /: origMap) {
    (resultMap: Map[V, Set[K]], pair: (K, V)) =>
      resultMap + ((pair._2, resultMap.getOrElse(pair._2, Set[K]()) + pair._1))
  }

  /**
    * Replace the origStr contains replaceMap characters contained in the Key content of the Value
    * 把origStr中的Key字符串替换成Value字符串
    *
    * @param origStr    要被替换的字符串
    * @param replaceMap 替换关系Map，把Key替换为Value
    * @return 替换结果
    */
  def replaceByKey(origStr: String, replaceMap: Map[String, String]): String = {
    var targetStr: String = origStr
    replaceMap.foreach(arg => targetStr = targetStr.replaceAll(arg._1, arg._2))
    targetStr
  }

  /**
    * 把origStr字符串中包含了replacedList中的元素字符串，统一替换为replaceValue
    *
    * @param origStr      要被替换的原始字符串
    * @param replacedList 要被替换的元素
    * @param replaceValue 要被统一替换的值
    * @return 替换结果
    */
  def replaceDefault(origStr: String, replacedList: List[String], replaceValue: String): String = {
    var targetStr: String = origStr
    replacedList.foreach(arg => targetStr = targetStr.replaceAll(arg, replaceValue))
    targetStr
  }

  /**
    * 把字符串按splitStr分割为List，然后将每个元素用aroundStr围绕，最后用linkStr连接为一个字符串
    * elem.split(",").map(e => e.mkString("'", "", "'")).mkString("", ",", "")
    * "A,B,C" => 'A','B','C'
    *
    * @param origStr   = "A,B,C"
    * @param splitStr  = ","  按splitStr分割字符串
    * @param aroundStr = "'" 将分割后的每个元素用该字符围绕
    * @param linkStr   = "," 用linkStr连接元素为一个字符串
    * @return = 'A','B','C'
    */
  def splitAroundLink(origStr: String, splitStr: String, aroundStr: String, linkStr: String): String = {
    origStr.split(splitStr).map(e => e.mkString(aroundStr, "", aroundStr)).mkString("", linkStr, "")
  }

  /**
    * 将List[String]中的每个字符串用splitAroundLink对其元素挨个处理，形成新的List
    * List("A,B,C","D,E,F") => List("'A','B','C'","'D','E','F'")
    *
    * @param orgiList = List("A,B,C","D,E,F")
    * @return = List("'A','B','C'","'D','E','F'")
    */
  def dealElemBySplitAroundLink(orgiList: List[String]): List[String] = {
    orgiList.map(e => splitAroundLink(e, ",", "'", ","))
  }

  /**
    * 将List[String]中每个元素都合并起来成为一个String，中间用splitStr拼接，最终在放到List[String]中，其中只有一个元素
    *
    * @param orgiList = List("'A'","'B'","'C'")
    * @param splitStr = "|"
    * @return List("'A'|'B'|'C'")
    */
  def dealElemMosaicOnlyOne(orgiList: List[String],splitStr: String): List[String] = {
    List(orgiList.mkString(splitStr))
  }

  /**
    * 将字符串转换为十六进制字符串
    */
  private val hexString: String = "0123456789ABCDEF"

  def encodeHex(str: String): String = {
    val bytes = str.getBytes()
    val sb = new StringBuilder(bytes.length * 2)
    for (i <- 0 until bytes.length) {
      sb.append(hexString.charAt((bytes(i) & 0xf0) >> 4))
      sb.append(hexString.charAt(bytes(i) & 0x0f))
    }
    sb.toString()
  }

  @throws[UnsupportedEncodingException]
  def strFromHex(hex: String): String = {
    val hex1: String = hex.replaceAll("^(00)+", "")
    val bytes = new Array[Byte](hex1.length / 2)
    var i = 0
    while (i < hex1.length) {
      {
        bytes(i / 2) = ((Character.digit(hex1.charAt(i), 16) << 4) + Character.digit(hex1.charAt(i + 1), 16)).toByte
      }
      i += 2
    }
    new String(bytes)
  }

  def processClassLoader[T](processClassName: String): T = {
    val classInstance = Class.forName(processClassName).newInstance().asInstanceOf[T]
    classInstance
  }
}

object Utils extends LogUtil {
  def apply(): Utils = {
    new Utils()
  }
}