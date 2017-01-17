package org.broadinstitute.hail.driver

import org.broadinstitute.hail.expr.{Fun, TStruct, TypeTag, FunctionMetadata, FunctionRegistry}
import org.broadinstitute.hail.utils._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.kohsuke.args4j.spi.OptionHandler
import org.kohsuke.args4j.{CmdLineParser, Option => Args4jOption}

import scala.collection.JavaConverters._

object FunctionRegistryDocs extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "output file location")
    var outputFile: String = _
  }

  def description = "Convert function registry to RestructuredText"

  def name = "docfunctions"

  override def hidden = true

  def requiresVDS = false

  def supportsMultiallelic = true

  def newOptions = new Options

  def run(state: State, options: Options): State = {
    state.hadoopConf.writeTextFile(options.outputFile) { out =>
      out.write(FunctionRegistry.generateDocumentation())
    }

    state
  }
}
