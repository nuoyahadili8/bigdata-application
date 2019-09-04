package com.teradata.bigdata.util.tools

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
private object UncaughtExceptionHandler extends Thread.UncaughtExceptionHandler with LogUtil {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      logError("Uncaught exception in thread " + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if (!Utils().inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(ExitCode.OOM)
        } else {
          System.exit(ExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(ExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(ExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
