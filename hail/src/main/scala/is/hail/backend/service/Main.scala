package is.hail.backend.service

import is.hail.HailContext
import org.apache.log4j
import org.apache.log4j.spi.{RootLogger, DefaultRepositorySelector}
import org.apache.log4j.{FileAppender, Hierarchy, Level, LogManager, Logger, PatternLayout, PropertyConfigurator}

import java.util.Properties
import scala.jdk.CollectionConverters._

object Main {
  val WORKER = "worker"
  val DRIVER = "driver"

  def configureLogging(logFile: String): Unit = {
    LogManager.getRootLogger().removeAllAppenders()
    val fa = new FileAppender()
    fa.setName("Hail File Appender: " + logFile)
    fa.setFile(logFile)
    fa.setLayout(new PatternLayout(HailContext.logFormat))
    fa.setThreshold(Level.INFO)
    fa.activateOptions()
    LogManager.getRootLogger().addAppender(fa)
    LogManager.getRootLogger().error("HELLO I WORK!!!!")
  }

//  def configureLogging(logFile: String): Unit = {
//    LogManager.getRootLogger().removeAllAppenders()
//    val fa = new FileAppender()
//    fa.setName("Hail File Appender: " + logFile)
//    fa.setFile(logFile)
//    fa.setLayout(new PatternLayout(HailContext.logFormat))
//    fa.setThreshold(Level.INFO)
//    fa.activateOptions()
//    LogManager.getRootLogger().addAppender(fa)
//  }

//  def configureLogging(logFile: String): Unit = {
//    val logProps = new Properties()
//
//    logProps.put("log4j.rootLogger", "INFO, logfile")
//    logProps.put("log4j.appender.logfile", "org.apache.log4j.FileAppender")
//    logProps.put("log4j.appender.logfile.append", true.toString)
//    logProps.put("log4j.appender.logfile.file", logFile)
//    logProps.put("log4j.appender.logfile.threshold", "INFO")
//    logProps.put("log4j.appender.logfile.layout", "org.apache.log4j.PatternLayout")
//    logProps.put("log4j.appender.logfile.layout.ConversionPattern", HailContext.logFormat)
//
////    with open(logFile, 'w') as f:
////      f.write(LogManager.getLoggerRepository.toString)
//
////    val logger = LogManager.getRootLogger()
////    logger.removeAllAppenders()
////    val fa = new FileAppender()
////    fa.setFile(logFile)
////    fa.setLayout(new PatternLayout(HailContext.logFormat))
////    fa.setThreshold(Level.INFO)
////    fa.activateOptions()
////
//////    LogManager.getLoggerRepository.shutdown()
////    LogManager.resetConfiguration()
////    LogManager.getRootLogger.addAppender(fa)
////
//
//
////    logger.addAppender(fa)
//
////    val appender = new FileAppender(SimpleLayout, logFile, false)
////
////    for (logger <- LogManager.getCurrentLoggers.asScala) {
////      logger.asInstanceOf[Logger].removeAllAppenders()
////      logger.asInstanceOf[Logger].addAppender(fa)
////    }
//
//    val h = new Hierarchy(new RootLogger(Level.INFO));
//    val repositorySelector = new DefaultRepositorySelector(h);
//    LogManager.setRepositorySelector(repositorySelector, null)
//    LogManager.resetConfiguration()
//    PropertyConfigurator.configure(logProps)
//  }

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
