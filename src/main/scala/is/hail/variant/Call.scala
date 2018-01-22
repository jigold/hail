package is.hail.variant

import java.io.Serializable

import is.hail.check.Gen
import is.hail.utils._
import is.hail.variant.GenotypeType.GenotypeType

object Call extends Serializable {

  def apply(call: java.lang.Integer): Call = {
    require(call == null || call >= 0, s"Call must be null or >= 0. Found ${ call }.")
    call
  }

  def encode(j: Int, k: Int, phased: Boolean, ploidy: Int): Int = {
    val gt =
      if (phased)
        ???
      else
        Genotype.gtIndexWithSwap(j, k)

    encode(gt, phased, ploidy)
  }

  def encode(i: Int, phased: Boolean = false, ploidy: Int = 2): Int = {
    require(ploidy >= 0, s"invalid ploidy: $ploidy")

    var call = 0
    call |= phased.toInt

    if (ploidy > 2)
      call |= (3 << 1)
    else
      call |= (ploidy << 1)

    require((i >>> 29) == 0, s"invalid gt: $i. Max value is 2^29 - 1")
    call |= i << 3

    call
  }

  def isPhased(call: Call): Boolean = call != null && (call & 0x01) == 1

  def ploidy(call: Call): java.lang.Integer = {
    if (call == null)
      null
    else
      (call >>> 1) & 0x03
  }

  def gtIndex(call: Call): java.lang.Integer = {
    if (call == null)
      null
    else
      call >>> 3
  }

  def toString(call: Call): String =
    if (call == null)
      "./."
    else {
      val p = Genotype.gtPair(gtIndex(call))
      s"${ p.j }/${ p.k }"
    }

  def check(call: Call, nAlleles: Int) {
    val nGenotypes = triangle(nAlleles)
    assert(call == null || (gtIndex(call) >= 0 && gtIndex(call) < nGenotypes), s"Invalid genotype found `$call' for number of alleles equal to `$nAlleles'.")
  }

  def genArb: Gen[Call] =
    for {
      v <- Variant.gen
      nAlleles = v.nAlleles
      nGenotypes = triangle(nAlleles)
      callOption <- Gen.option(Gen.choose(0, nGenotypes - 1))
      call = callOption.map(_.asInstanceOf[java.lang.Integer]).orNull
    } yield {
      check(call, nAlleles)
      call
    }

  def genNonmissingValue: Gen[Call] =
    for {
      v <- Variant.gen
      nAlleles = v.nAlleles
      nGenotypes = triangle(nAlleles)
      c <- Gen.choose(0, nGenotypes - 1)
      call = c.asInstanceOf[java.lang.Integer]
    } yield {
      check(call, nAlleles)
      call
    }

  def isHomRef(call: Call): Boolean = call != null && gtIndex(call) == 0

  def isHet(call: Call): Boolean = call != null && gtIndex(call) > 0 && {
    val p = Genotype.gtPair(call)
    p.j != p.k
  }

  def isHomVar(call: Call): Boolean = call != null && gtIndex(call) > 0 && {
    val p = Genotype.gtPair(gtIndex(call))
    p.j == p.k
  }

  def isNonRef(call: Call): Boolean = call != null && gtIndex(call) > 0

  def isHetNonRef(call: Call): Boolean = call != null && gtIndex(call) > 0 && {
    val p = Genotype.gtPair(gtIndex(call))
    p.j > 0 && p.j != p.k
  }

  def isHetRef(call: Call): Boolean = call != null && gtIndex(call) > 0 && {
    val p = Genotype.gtPair(gtIndex(call))
    p.j == 0 && p.k > 0
  }

  def gtType(call: Call): GenotypeType =
    if (isHomRef(call))
      GenotypeType.HomRef
    else if (isHet(call))
      GenotypeType.Het
    else if (isHomVar(call))
      GenotypeType.HomVar
    else {
      assert(call == null)
      GenotypeType.NoCall
    }

  def gtj(call: Call): java.lang.Integer =
    if (call == null)
      null
    else
      box(Genotype.gtPair(gtIndex(call)).j)

  def gtk(call: Call): java.lang.Integer =
    if (call == null)
      null
    else
      box(Genotype.gtPair(gtIndex(call)).k)

  def nNonRefAlleles(call: Call): java.lang.Integer =
    if (call != null)
      Genotype.gtPair(gtIndex(call)).nNonRefAlleles
    else
      null

  def oneHotAlleles(call: Call, nAlleles: Int): IndexedSeq[Int] = {
    if (call != null) {
      val gtPair = Genotype.gtPair(gtIndex(call))
      val j = gtPair.j
      val k = gtPair.k
      new IndexedSeq[Int] with Serializable {
        def length: Int = nAlleles

        def apply(idx: Int): Int = {
          if (idx < 0 || idx >= nAlleles)
            throw new ArrayIndexOutOfBoundsException(idx)
          var r = 0
          if (idx == j)
            r += 1
          if (idx == k)
            r += 1
          r
        }
      }
    } else null
  }

  def oneHotGenotype(call: Call, nGenotypes: Int): IndexedSeq[Int] = {
    if (call != null) {
      new IndexedSeq[Int] with Serializable {
        def length: Int = nGenotypes

        def apply(idx: Int): Int = {
          if (idx < 0 || idx >= nGenotypes)
            throw new ArrayIndexOutOfBoundsException(idx)
          if (idx == gtIndex(call))
            1
          else
            0
        }
      }
    } else null
  }

  def oneHotAlleles(call: Call, v: Variant): IndexedSeq[Int] = oneHotAlleles(call, v.nAlleles)

  def oneHotGenotype(call: Call, v: Variant): IndexedSeq[Int] = oneHotGenotype(call, v.nGenotypes)
}
