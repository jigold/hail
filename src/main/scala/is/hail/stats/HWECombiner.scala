package is.hail.stats

import is.hail.annotations.Annotation
import is.hail.expr.{TDouble, TStruct}
import is.hail.utils._
import is.hail.variant.Genotype

object HWECombiner {
  def makeSchema: (TStruct, TStruct) = TStruct.applyWDocstring(
    ("rExpectedHetFrequency", TDouble, "Expected rHeterozygosity based on Hardy Weinberg Equilibrium"),
    ("pHWE", TDouble, "p-value")
  )

  def signature = makeSchema._1

  def annMetadata = makeSchema._2
}

class HWECombiner extends Serializable {
  var nHomRef = 0
  var nHet = 0
  var nHomVar = 0

  def merge(gt:Genotype): HWECombiner = {
    if (gt.isHomRef)
      nHomRef += 1
    else if (gt.isHet)
      nHet += 1
    else if (gt.isHomVar)
      nHomVar += 1

    this
  }

  def merge(other: HWECombiner): HWECombiner = {
    nHomRef += other.nHomRef
    nHet += other.nHet
    nHomVar += other.nHomVar

    this
  }

  def n = nHomRef + nHet + nHomVar
  def nA = nHet + 2 * nHomRef.min(nHomVar)

  def lh = LeveneHaldane(n, nA)

  def asAnnotation: Annotation = Annotation(divOption(lh.getNumericalMean, n).orNull, lh.exactMidP(nHet))
}
