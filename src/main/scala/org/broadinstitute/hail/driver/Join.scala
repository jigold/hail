package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.{EvalContext, Parser, TAggregable, TBoolean, TGenotype, TSample, TVariant}
import org.broadinstitute.hail.methods.{Aggregators, Filter}
import org.broadinstitute.hail.variant.{Genotype, Variant}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
  * Created by laurent on 6/27/16.
  */
object Join extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-in", aliases = Array("--input"),
      usage = "VDS to join")
    var condition: String = _

  }

  def newOptions = new Options

  def name = "join"

  def description = "Inner join two VDS"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {
    val vds = state.vds


    state
  }
}
