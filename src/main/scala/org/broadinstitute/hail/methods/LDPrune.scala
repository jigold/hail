package org.broadinstitute.hail.methods

import java.util

import breeze.linalg.{Vector => BVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.expr.{TBoolean, TStruct}
import org.broadinstitute.hail.sparkextras._
import org.broadinstitute.hail.variant.{Genotype, Locus, Variant, VariantDataset}
import org.broadinstitute.hail.utils._

case class LocalPruneResult(rdd: OrderedRDD[Locus, Variant, BVector[Double]],
  fractionPruned: Double, index: Int, partitionSizes: Array[Long], pruneDone: Boolean) {

  def nVariants = partitionSizes.sum

  def nPartitions = partitionSizes.length
}

case class GlobalPruneIntermediate(rdd: GeneralRDD[(Variant, BVector[Double])], index: Int, persist: Boolean)

object LDPrune {
  val variantByteOverhead = 50
  val fractionMemoryToUse = 0.25

  def toNormalizedGtArray(gs: Iterable[Genotype], nSamples: Int): Option[Array[Double]] = {
    val a = new Array[Double](nSamples)
    val gts = new Array[Int](nSamples)
    val it = gs.iterator

    var nPresent = 0
    var gtSum = 0
    var gtSumSq = 0

    var i = 0
    while (i < nSamples) {
      val gt = it.next().unboxedGT
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

  def pruneGlobal(rdd: OrderedRDD[Locus, Variant, BVector[Double]], r2Threshold: Double, window: Int) = {
    val rangePartitioner = rdd.orderedPartitioner
    val rangeBounds = rangePartitioner.rangeBounds
    val partitionIndices = rdd.getPartitions.map(_.index)
    val nPartitions = rdd.partitions.length

    def computeDependencies(partitionId: Int): Array[Int] = {
      if (partitionId == partitionIndices(0))
        Array(partitionId)
      else {
        val startLocus = rangeBounds(partitionId - 1) // this is the best estimate of the first locus of the partition
        val minimumLocus = Locus(startLocus.contig, math.max(startLocus.position - window, 0))
        val partMinimum = rangePartitioner.getPartitionT(minimumLocus)
        partitionIndices.filter(idx => idx >= partMinimum && idx <= partitionId).reverse
      }
    }

    def pruneF = (x: Array[Iterator[(Variant, BVector[Double])]]) => {
      val nPartitions = x.length
      val targetIterator = x(0)
      val prevPartitions = x.drop(1).reverse

      if (nPartitions == 1)
        targetIterator
      else {
        var targetData = targetIterator.toArray

        prevPartitions.foreach { it =>
          it.foreach { case (v2, sgs2) =>
            targetData = targetData.filter { case (v, sgs) =>
              if (v.contig != v2.contig || v.start - v2.start > window)
                true
              else {
                val r = sgs.dot(sgs2)
                (r * r: Double) < r2Threshold
              }
            }
          }
        }
        targetData.iterator
      }
    }

    val contigStartPartitions =
      partitionIndices.filter { i =>
        i == partitionIndices(0) || i == partitionIndices.last || rangeBounds(i).contig != rangeBounds(i - 1).contig
      }

    val pruneIntermediates = Array.fill[GlobalPruneIntermediate](nPartitions)(null)

    def generalRDDInputs(partitionIndex: Int) = {
      computeDependencies(partitionIndex).zipWithIndex.map { case (depIndex, i) =>
        if (depIndex == partitionIndex || contigStartPartitions.contains(depIndex))
          (rdd, (i, depIndex))
        else {
          val gpi = pruneIntermediates(depIndex)
          pruneIntermediates(depIndex) = gpi.copy(persist = true)
          (gpi.rdd, (i, gpi.index))
        }
      }.unzip
    }

    val sc = rdd.sparkContext

    for (i <- partitionIndices) {
      val (rdds, inputs) = generalRDDInputs(i)
      pruneIntermediates(i) = GlobalPruneIntermediate(rdd = new GeneralRDD(sc, rdds, Array((inputs, pruneF))), index = 0, persist = false) // creating single partition RDDs with partition index = 0
    }

    pruneIntermediates.foreach { gpi =>
      if (gpi.persist) gpi.rdd.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val prunedRDD = new GeneralRDD[(Variant, BVector[Double])](sc, pruneIntermediates.map(_.rdd),
      pruneIntermediates.zipWithIndex.map { case (gpi, i) =>
        (Array((i, gpi.index)), pruneF)
      }).persist(StorageLevel.MEMORY_AND_DISK)

    (prunedRDD, pruneIntermediates)
  }

  def estimateMemoryRequirements(nVariants: Long, nSamples: Int, memoryPerCore: Long) = {
    val numBytesPerVariant = 8 * nSamples + variantByteOverhead
    val memoryAvailPerCore = memoryPerCore * fractionMemoryToUse

    val maxQueueSize = math.max(1, math.ceil(memoryAvailPerCore / numBytesPerVariant).toInt)
    val numPartitionsRequired = math.max(1, math.ceil(nVariants.toDouble / maxQueueSize).toInt)

    assert(maxQueueSize > 0 && numPartitionsRequired > 0)

    (maxQueueSize, numPartitionsRequired)
  }

  def ldPrune(vds: VariantDataset, dest: String,
    r2Threshold: Double, window: Int,
    localPruneThreshold: Double, memoryPerCore: Long = 1073741824) = {

    val nSamples = vds.nSamples
    val partitionSizesInitial = vds.sparkContext.runJob(vds.rdd, getIteratorSize _)
    val nVariantsInitial = partitionSizesInitial.sum

    info(s"InputData: nSamples=$nSamples nVariants=$nVariantsInitial nPartitions=${ partitionSizesInitial.length }")

    val minMemoryPerCore = math.ceil((1 / fractionMemoryToUse) * 8 * nSamples + variantByteOverhead)
    val (maxQueueSize, _) = estimateMemoryRequirements(nVariantsInitial, nSamples, memoryPerCore)

    if (localPruneThreshold < 0 || localPruneThreshold > 1)
      fatal(s"Local prune threshold must be in the range [0,1]. Found `$localPruneThreshold'.")

    if (r2Threshold < 0 || r2Threshold > 1)
      fatal(s"R^2 threshold must be in the range [0,1]. Found `$r2Threshold'.")

    if (window < 0)
      fatal(s"Window must be greater than or equal to 0. Found `$window'.")

    if (memoryPerCore < minMemoryPerCore)
      fatal(s"Memory per core must be greater than ${ minMemoryPerCore / (1024 * 1024) }MB")

    def pruneLocal(input: LocalPruneResult, queueSize: Option[Int]): LocalPruneResult = {

      val repartitionRequired = queueSize.isDefined && input.partitionSizes.exists(_ > maxQueueSize)

      val prunedRDD = input.rdd.mapPartitions({ it =>
        val queue = queueSize match {
          case Some(qs) => new util.ArrayDeque[(Variant, BVector[Double])](qs)
          case None => new util.ArrayDeque[(Variant, BVector[Double])]
        }

        it.filter { case (v, sgs) =>
          var keepVariant = true
          var done = false
          val qit = queue.descendingIterator()

          while (!done && qit.hasNext) {
            val (v2, sgs2) = qit.next()
            if (v.contig != v2.contig || v.start - v2.start > window)
              done = true
            else {
              val r = sgs.dot(sgs2)
              if ((r * r: Double) >= r2Threshold) {
                keepVariant = false
                done = true
              }
            }
          }

          if (keepVariant) {
            queue.addLast((v, sgs))
            queueSize.foreach { qs =>
              if (queue.size() > qs) {
                queue.pop()
              }
            }
          }

          keepVariant
        }
      }, preservesPartitioning = true).asOrderedRDD.persist(StorageLevel.MEMORY_AND_DISK)

      val partitionSizes = prunedRDD.sparkContext.runJob(prunedRDD, getIteratorSize _)
      prunedRDD.count()
      val nVariantsKept = partitionSizes.sum

      input.rdd.unpersist()

      val fractionPruned = 1.0 - nVariantsKept.toDouble / input.nVariants
      assert(fractionPruned >= 0.0 && fractionPruned < 1.0)

      val result = LocalPruneResult(prunedRDD, fractionPruned, input.index + 1, partitionSizes, pruneDone = true)

      if (repartitionRequired || fractionPruned > localPruneThreshold) {
        val nPartitions = estimateMemoryRequirements(nVariantsKept, nSamples, memoryPerCore)._2
        val repartRDD = prunedRDD.repartition(nPartitions)(null).asOrderedRDD
        repartRDD.persist(StorageLevel.MEMORY_AND_DISK)
        repartRDD.count()
        val partitionSizesRepart = repartRDD.sparkContext.runJob(repartRDD, getIteratorSize _)
        prunedRDD.unpersist()

        result.copy(rdd = repartRDD, partitionSizes = partitionSizesRepart, pruneDone = false)
      } else
        result
    }

    val standardizedRDD = vds.rdd.flatMapValues { case (va, gs) =>
      toNormalizedGtArray(gs, nSamples).map(BVector(_))
    }.asOrderedRDD

    var oldResult = LocalPruneResult(standardizedRDD, 0.0, 0, partitionSizesInitial, pruneDone = false)
    var (newResult, duration) = time(pruneLocal(oldResult, Option(maxQueueSize)))

    info(s"Local Prune ${ newResult.index }: fractionPruned=${ newResult.fractionPruned } nVariantsRemaining=${ newResult.nVariants } nPartitions=${newResult.nPartitions} time=${ formatTime(duration) }")

    while (!newResult.pruneDone) {
      oldResult = newResult
      val (result, duration) = time(pruneLocal(oldResult, None))
      newResult = result
      info(s"Local Prune ${ newResult.index }: fractionPruned=${ newResult.fractionPruned } nVariantsRemaining=${ newResult.nVariants } nPartitions=${newResult.nPartitions} time=${ formatTime(duration) }")
    }

    val (finalPrunedRDD, globalPruneIntermediates) = pruneGlobal(newResult.rdd, r2Threshold, window)
    val (nVariantsGlobal, globalDuration) = time(finalPrunedRDD.count())
    info(s"Global Prune: nVariantsRemaining=$nVariantsGlobal time=${ formatTime(globalDuration) }")

    val annotatedRDD = finalPrunedRDD.mapValues(_ => Annotation(true))
    val schema = TStruct("prune" -> TBoolean)

    val result = vds.annotateVariants(annotatedRDD.orderedRepartitionBy(vds.rdd.orderedPartitioner), schema, dest)

    newResult.rdd.unpersist()
    globalPruneIntermediates.foreach { gpi => gpi.rdd.unpersist() }
    finalPrunedRDD.unpersist()

    result
  }
}