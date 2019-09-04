package com.teradata.bigdata.util.tools

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.impl.StaticLoggerBinder

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
trait LogUtil {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = _

  // Method to get the logger name for this object
  protected def logName: String = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log:Logger = {
    if (log_ == null) {
      initializeIfNecessary()
      /*var className = this.getClass.getName
      // Ignore trailing $'s in the class names for Scala objects
      if (className.endsWith("$")) {
        className = className.substring(0, className.length - 1)
      }*/
      //log_ = LoggerFactory.getLogger(className)
      log_ = LoggerFactory.getLogger(logName)
    }
    //println("log 的开关状态"+log_.isDebugEnabled)
    log_
  }


  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled: Boolean = {
    log.isTraceEnabled
  }


  private def initializeIfNecessary() {
    if (!LogUtil.initialized) {
      LogUtil.initLock.synchronized {
        if (!LogUtil.initialized) {
          initializeLogging()
        }
      }
    }
  }

  //
  /*private def initializeLogging() {
    // If Log4j is being used, but is not initialized, load a default properties file
    val binder = StaticLoggerBinder.getSingleton
    val usingLog4j = binder.getLoggerFactoryClassStr.endsWith("Log4jLoggerFactory")
    val log4jInitialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
    println("usingLog4j 状态" + usingLog4j)
    println("log4jInitialized 状态" + log4jInitialized)
    if (!log4jInitialized && usingLog4j) {
      val defaultLogProps = "openapi-log4j-defaults.properties"
      Option(Utils.getOpenApiClassLoader.getResource(defaultLogProps)) match {
        case Some(url) =>
          PropertyConfigurator.configure(url)
          log.info(s"Using OpenApi's default log4j profile: $defaultLogProps")
        case None =>
          System.err.println(s"OpenApi was unable to load $defaultLogProps")
      }
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from mutliple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }*/

  private def initializeLogging() {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    println("binderClass 名称" + binderClass)
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory

    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    //val usingLog4j16 = "org.slf4j.helpers.NOPLoggerFactory".equals(binderClass)
    println("usingLog4j12 状态" + usingLog4j12)
    if (usingLog4j12) {
      val log4j16Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      println("log4j16Initialized 状态" + log4j16Initialized)
      if (!log4j16Initialized) {
        val defaultLogProps = "log4j.properties"
        Option(Utils().getContextOrIopClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Openapi's default log4j profile: $defaultLogProps")
          case None =>
            System.err.println(s"Openapi was unable to load $defaultLogProps")
        }
      }
    }
    LogUtil.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private object LogUtil{
  @volatile private var initialized = false
  val initLock = new Object()

  try{
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  }catch{
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
