package is.hail.backend.service

import is.hail.HailContext
import org.apache.log4j.{FileAppender, Level, LogManager, PatternLayout}


object Main {
  val WORKER = "worker"
  val DRIVER = "driver"

  def configureLogging(logFile: String): Unit = {
    val logger = LogManager.getRootLogger()
    logger.removeAllAppenders()
    val fa = new FileAppender()
    fa.setFile(logFile)
    fa.setLayout(new PatternLayout(HailContext.logFormat))
    fa.setThreshold(Level.INFO)
    logger.addAppender(fa)
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
