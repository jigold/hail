package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.{EvalContext, Parser, TAggregable, TBoolean, TGenotype, TSample, TVariant}
import org.broadinstitute.hail.methods.{Aggregators, Filter}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
  * Created by laurent on 6/27/16.
  */
object Join extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-i", aliases = Array("--input"),
      usage = "VDS to join. If -sr is not specified, the sample annotations will be merged under the assumption that " +
        "annotations with the same name are the same in both VDSs.")
    var input: String = _
    @Args4jOption(required = true, name = "-vr", aliases = Array("--va_root"),
      usage = "Period-delimited path starting with `va'")
    var vr: String = _
    @Args4jOption(required = true, name = "-gr", aliases = Array("--ga_root"),
      usage = "Period-delimited path starting with `ga'")
    var gr: String = _
    @Args4jOption(required = false, name = "-sr", aliases = Array("--sa_root"),
      usage = "Period-delimited path starting with `sa'")
    var sr: String = "sa"


  }

  def newOptions = new Options

  def name = "join"

  def description = "Inner join two VDSs"

  def supportsMultiallelic = true

  def requiresVDS = true

  def run(state: State, options: Options): State = {

    state.copy(vds = state.vds.join(
      otherRDD = VariantSampleMatrix.read(state.sqlContext, options.input),
      vaPath = options.vr.split(",").toList,
      gaPath = options.gr.split(",").toList,
      saPath = options.sr.split(",").toList
    ))
  }
}
