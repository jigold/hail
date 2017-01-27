package is.hail.methods

import breeze.linalg.{Vector => BVector}
import is.hail.SparkSuite
import is.hail.check.Prop._
import is.hail.check.{Gen, Properties}
import is.hail.driver._
import is.hail.variant._
import is.hail.utils._
import org.testng.annotations.Test

class LDPruneSuite extends SparkSuite {
  val bytesPerCore = 256L * 1024L * 1024L

  def convertGtToGs(gts: Array[Int]): Iterable[Genotype] = gts.map(Genotype(_))

  def toNormalizedGtArray(gs: Array[Int], nSamples: Int): Option[Array[Double]] = {
    val a = new Array[Double](nSamples)
    val gts = new Array[Int](nSamples)
    val it = gs.iterator

    var nPresent = 0
    var gtSum = 0
    var gtSumSq = 0

    var i = 0
    while (i < nSamples) {
      val gt = it.next()
      gts.update(i, gt)
      if (gt >= 0) {
        nPresent += 1
        gtSum += gt
        gtSumSq += gt * gt
      }
      i += 1
    }

    val nMissing = nSamples - nPresent
    val allHomRef = gtSum == 0
    val allHet = gtSum == nPresent && gtSumSq == nPresent
    val allHomVar = gtSum == 2 * nPresent

    if (allHomRef || allHet || allHomVar || nMissing == nSamples)
      None
    else {
      val gtMean = gtSum.toDouble / nPresent
      val gtMeanAll = (gtSum + nMissing * gtMean) / nSamples
      val gtMeanSqAll = (gtSumSq + nMissing * gtMean * gtMean) / nSamples
      val gtStdDevRec = 1d / math.sqrt((gtMeanSqAll - gtMeanAll * gtMeanAll) * nSamples)

      var i = 0
      while (i < nSamples) {
        val gt = gts(i)
        if (gt >= 0)
          a.update(i, (gt - gtMean) * gtStdDevRec)
        i += 1
      }

      Some(a)
    }
  }

  def correlationMatrix(gs: Array[Iterable[Genotype]], nSamples: Int) = {
    val bvi = gs.map(LDPrune.toBitPackedVector(_, nSamples))
    val r2 = for (i <- bvi.indices; j <- bvi.indices) yield {
      (bvi(i), bvi(j)) match {
        case (Some(x), Some(y)) =>
          Some(LDPrune.r2(x, y))
        case _ => None
      }
    }
    val nVariants = bvi.length
    new MultiArray2(nVariants, nVariants, r2.toArray)
  }

  def uncorrelated(vds: VariantDataset, r2Threshold: Double, window: Int): Boolean = {
    val nSamplesLocal = vds.nSamples
    val r2Matrix = correlationMatrix(vds.rdd.map { case (v, (va, gs)) => gs }.collect(), nSamplesLocal)
    val variantMap = vds.variants.zipWithIndex().map { case (v, i) => (i.toInt, v) }.collectAsMap()

    r2Matrix.indices.forall { case (i, j) =>
      val v1 = variantMap(i)
      val v2 = variantMap(j)
      val r2 = r2Matrix(i, j)

      v1 == v2 ||
        v1.contig != v2.contig ||
        (v1.contig == v2.contig && math.abs(v1.start - v2.start) > window) ||
        r2.exists(_ < r2Threshold)
    }
  }

  @Test def testBitPackUnpack() {
    val gts1 = Array(-1, 0, 1, 2, 1, 1, 0, 0, 0, 0, 2, 2, -1, -1, -1, -1)
    val gts2 = Array(0, 1, 2, 2, 2, 0, -1, -1)
    val gts3 = gts1 ++ Array.fill[Int](32 - gts1.length)(0) ++ gts2

    for (gts <- Array(gts1, gts2, gts3)) {
      val n = gts.length
      assert(LDPrune.toBitPackedVector(convertGtToGs(gts), n).forall { bpv =>
        println(s"res=${bpv.unpack().mkString(",")}")
        bpv.unpack() sameElements gts
      })
    }
  }

  @Test def testR2() {
    val gts = Array(
      Array(1, 0, 0, 0, 0, 0, 0, 0),
      Array(1, 1, 1, 1, 1, 1, 1, 1),
      Array(1, 2, 2, 2, 2, 2, 2, 2),
      Array(1, 0, 0, 0, 1, 1, 1, 1),
      Array(1, 0, 0, 0, 1, 1, 2, 2),
      Array(1, 0, 1, 1, 2, 2, 0, 1),
      Array(1, 0, 1, 0, 2, 2, 1, 1)
    )

    val actualR2 = new MultiArray2(7, 7, hadoopConf.readLines("src/test/resources/ldprune_corrtest.txt")(_.flatMap(_.map { line =>
      line.trim.split("\t").map(r2 => if (r2 == "NA") None else Some(r2.toDouble))
    }.value).toArray))

    val computedR2 = correlationMatrix(gts.map(convertGtToGs), 8)

    val res = actualR2.indices.forall { case (i, j) =>
      val expected = actualR2(i, j)
      val computed = computedR2(i, j)

      (computed, expected) match {
        case (Some(x), Some(y)) =>
          val res = math.abs(x - y) < 1e-6
          if (!res)
            info(s"i=$i j=$j r2Computed=$x r2Expected=$y")
          res
        case (None, None) => true
        case _ =>
          info(s"i=$i j=$j r2Computed=$computed r2Expected=$expected")
          false
      }
    }

    assert(res)

    val input = Array(0, 1, 2, 2, 2, 0, -1, -1)
    val gs = convertGtToGs(input)
    val n = input.length
    val bvi1 = LDPrune.toBitPackedVector(gs, n).get
    val bvi2 = LDPrune.toBitPackedVector(gs, n).get

    assert(math.abs(LDPrune.r2(bvi1, bvi2) - 1d) < 1e-4)
  }

  @Test def testIdenticalVariants() {
    var s = State(sc, sqlContext, null)
    s = ImportVCF.run(s, Array("-i", "src/test/resources/ldprune2.vcf", "-n", "2"))
    s = SplitMulti.run(s, Array.empty[String])
    val prunedVds = LDPrune.ldPrune(s.vds, 0.2, 700, bytesPerCore)
    assert(prunedVds.nVariants == 1)
  }

  @Test def testMultipleChr() = {
    val r2 = 0.2
    val window = 500

    var s = State(sc, sqlContext, null)
    s = ImportVCF.run(s, Array("-i", "src/test/resources/ldprune_multchr.vcf", "-n", "10"))
    s = SplitMulti.run(s, Array.empty[String])
    val prunedVds = LDPrune.ldPrune(s.vds, r2, window, bytesPerCore)

    assert(uncorrelated(prunedVds, r2, window))
  }

  object Spec extends Properties("LDPrune") {
    val compGen = for (r2: Double <- Gen.choose(0.5, 1.0);
      window: Int <- Gen.choose(0, 5000);
      numPartitions: Int <- Gen.choose(5, 10)) yield (r2, window, numPartitions)

    val vectorGen = for (nSamples: Int <- Gen.choose(1, 10000);
      v1: Array[Int] <- Gen.buildableOfN[Array, Int](nSamples, Gen.choose(-1, 2));
      v2: Array[Int] <- Gen.buildableOfN[Array, Int](nSamples, Gen.choose(-1, 2))
    ) yield (nSamples, v1, v2)

    property("bitPacked same as BVector") =
      forAll(vectorGen) { case (nSamples: Int, v1: Array[Int], v2: Array[Int]) =>
        val gs1 = convertGtToGs(v1)
        val gs2 = convertGtToGs(v2)
        val bv1 = LDPrune.toBitPackedVector(gs1, nSamples)
        val bv2 = LDPrune.toBitPackedVector(gs2, nSamples)
        val sgs1 = toNormalizedGtArray(v1, nSamples).map(BVector(_))
        val sgs2 = toNormalizedGtArray(v2, nSamples).map(BVector(_))

        val res2 = (bv1, bv2, sgs1, sgs2) match {
          case (Some(a), Some(b), Some(c), Some(d)) =>
            val rBreeze = c.dot(d): Double
            val r2Breeze = rBreeze * rBreeze
            val r2BitPacked = LDPrune.r2(a, b)
            val res = math.abs(r2BitPacked - r2Breeze) < 1e-4
            if (!res)
              println(s"breeze=$r2Breeze bitPacked=$r2BitPacked nSamples=$nSamples v1=${v1.mkString(",")} v2=${v2.mkString(",")}")
            res
          case (_, _, _, _) => true
        }
        res2
      }

//    property("uncorrelated") =
//      forAll(compGen) { case (r2: Double, window: Int, numPartitions: Int) =>
//        var s = State(sc, sqlContext, null)
//        s = ImportVCF.run(s, Array("-i", "src/test/resources/sample.vcf.bgz", "-n", s"$numPartitions"))
//        s = SplitMulti.run(s, Array.empty[String])
//        val prunedVds = LDPrune.ldPrune(s.vds, r2, window, bytesPerCore)
//        uncorrelated(prunedVds, r2, window)
//      }
  }

  @Test def testRandom() {
    Spec.check2()
  }

  @Test def testInputs() {
    def setup() = {
      var s = State(sc, sqlContext, null)
      s = ImportVCF.run(s, Array("-i", "src/test/resources/sample.vcf.bgz", "-n", "10"))
      SplitMulti.run(s, Array.empty[String])
    }

    // memory per core requirement
    intercept[FatalException] {
      val s = setup()
      s.copy(vds = LDPrune.ldPrune(s.vds, 0.2, 1000, 0))
    }

    // r2 negative
    intercept[FatalException] {
      val s = setup()
      s.copy(vds = LDPrune.ldPrune(s.vds, -0.1, 1000, 1000))
    }

    // r2 > 1
    intercept[FatalException] {
      val s = setup()
      val prunedVds = LDPrune.ldPrune(s.vds, 1.1, 1000, 1000)
    }

    // window negative
    intercept[FatalException] {
      val s = setup()
      s.copy(vds = LDPrune.ldPrune(s.vds, 0.5, -2, 1000))
    }
  }

  @Test def testMemoryRequirements() {
    val a = LDPrune.estimateMemoryRequirements(nVariants = 1, nSamples = 1, memoryPerCore = 512 * 1024 * 1024)
    assert(a._2 == 1)

    val nSamples = 5
    val nVariants = 5
    val memoryPerVariant = LDPrune.variantByteOverhead + math.ceil(nSamples.toDouble / LDPrune.genotypesPerPack).toLong
    val recipFractionMemoryUsed = 1.0 / LDPrune.fractionMemoryToUse
    val memoryPerCore = math.ceil(memoryPerVariant * recipFractionMemoryUsed).toInt

    for (i <- 1 to nVariants) {
      val y = LDPrune.estimateMemoryRequirements(nVariants, nSamples, memoryPerCore * i)
      assert(y._1 == i && y._2 == math.ceil(nVariants.toDouble / i).toInt)
    }
  }

  @Test def testWindow() {
    var s = State(sc, sqlContext, null)
    s = ImportVCF.run(s, Array("-i", "src/test/resources/sample.vcf.bgz"))
    s = SplitMulti.run(s, Array.empty[String])
    val prunedVds = LDPrune.ldPrune(s.vds, 0.2, 100000, 200000)
    assert(uncorrelated(prunedVds, 0.2, 1000))
  }

  @Test def test10K() {
    var s = State(sc, sqlContext, null)
    s = Read.run(s, Array("ALL.1KG.10K.vds"))
    val prunedVds = LDPrune.ldPrune(s.vds, 0.2, 1000000, 20 * 1024 * 1024)
    prunedVds.nVariants
    // while (true) {}
  }

  @Test def test100K() {
    var s = State(sc, sqlContext, null)
    s = Read.run(s, Array("1000Genomes.ALL.coreExome100K.updated.vds"))
    val prunedVds = LDPrune.ldPrune(s.vds, 0.2, 1000000, 256 * 1024 * 1024)
    prunedVds.nVariants
    while (true) {}
  }
}
