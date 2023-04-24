package is.hail.backend.service

import is.hail.HailContext
import org.apache.log4j
import org.apache.log4j.{FileAppender, PatternLayout, Level, LogManager, Logger, PropertyConfigurator}

import java.util.Properties
import scala.jdk.CollectionConverters._

object Main {
  val WORKER = "worker"
  val DRIVER = "driver"

  def configureLogging(logFile: String): Unit = {
    val logProps = new Properties()

    logProps.put("log4j.rootLogger", "INFO, logfile")
    logProps.put("log4j.appender.logfile", "org.apache.log4j.FileAppender")
    logProps.put("log4j.appender.logfile.append", true.toString)
    logProps.put("log4j.appender.logfile.file", logFile)
    logProps.put("log4j.appender.logfile.threshold", "INFO")
    logProps.put("log4j.appender.logfile.layout", "org.apache.log4j.PatternLayout")
    logProps.put("log4j.appender.logfile.layout.ConversionPattern", HailContext.logFormat)

//    val logger = LogManager.getRootLogger()
//    logger.removeAllAppenders()
    val fa = new FileAppender()
    fa.setFile(logFile)
    fa.setLayout(new PatternLayout(HailContext.logFormat))
    fa.setThreshold(Level.INFO)
//    logger.addAppender(fa)

//    val appender = new FileAppender(SimpleLayout, logFile, false)
//
    for (logger <- LogManager.getCurrentLoggers.asScala) {
      logger.asInstanceOf[Logger].removeAllAppenders()
      logger.asInstanceOf[Logger].addAppender(fa)
    }

//    LogManager.resetConfiguration()
//    PropertyConfigurator.configure(logProps)
  }

  def main(argv: Array[String]): Unit = {
    val logFile = argv(1)
    configureLogging(logFile)

    argv(3) match {
      case WORKER => Worker.main(argv)
      case DRIVER => ServiceBackendSocketAPI2.main(argv)
      case kind => throw new RuntimeException(s"unknown kind: ${kind}")
    }
  }
}
