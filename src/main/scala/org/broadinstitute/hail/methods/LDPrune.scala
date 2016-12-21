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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Stage(index: Int, prunePartitions: Array[Int])

case class LocalPruneResult(rdd: OrderedRDD[Locus, Variant, BVector[Double]],
  fractionPruned: Double, index: Int, numVariants: Long)

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

  def pruneLocal(prevResult: LocalPruneResult, window: Int, r2Threshold: Double,
    localPruneThreshold: Double, queueSize: Option[Int], coalesce: Boolean): LocalPruneResult = {

    val prunedRDD = prevResult.rdd.mapPartitions({ it =>
      val queue = queueSize match {
        case Some(qs) => new util.ArrayDeque[(Variant, BVector[Double])](qs)
        case None => new util.ArrayDeque[(Variant, BVector[Double])]
      }

      it.filter { case (v, sgs) =>
        var keep = true
        var compare = true
        val qit = queue.descendingIterator()

        while (compare && qit.hasNext) {
          val (v2, sgs2) = qit.next()
          if (v.contig != v2.contig || v.start - v2.start > window)
            compare = false
          else {
            val r = sgs.dot(sgs2)
            if ((r * r: Double) >= r2Threshold) {
              keep = false
              compare = false
            }
          }
        }

        if (keep) {
          queue.addLast((v, sgs))
          queueSize.foreach { qs =>
            if (queue.size() > qs) {
              queue.pop()
            }
          }
        }

        keep
      }
    }, preservesPartitioning = true).asOrderedRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val numVariantsOld = prevResult.numVariants
    val numVariantsNew = prunedRDD.count()

    prevResult.rdd.unpersist()

    val fractionRemaining = numVariantsNew.toDouble / numVariantsOld
    assert(fractionRemaining >= 0.0 && fractionRemaining <= 1.0)

    val fractionPruned = 1.0 - fractionRemaining

    val numPartitionsOld = prevResult.rdd.partitions.length
    val numPartitionsNew = math.max(1, math.ceil(numPartitionsOld * fractionRemaining).toInt)

    info(s"coalesce=$coalesce numPartitionsOld=$numPartitionsOld numPartitionsNew=$numPartitionsNew fractionPruned=$fractionPruned localPruneThreshold=$localPruneThreshold")

    val newRDD =
      if (coalesce && numPartitionsOld != numPartitionsNew && fractionPruned > localPruneThreshold) {
        info(s"coalesced data")
        val result = prunedRDD.coalesce(numPartitionsNew, shuffle = true)(null).asOrderedRDD
        result.count()
        prunedRDD.unpersist()
        result
      }
      else
        prunedRDD

    LocalPruneResult(newRDD,
      fractionPruned,
      prevResult.index + 1,
      numVariantsNew)
  }

//  def pruneGlobal(rdd: OrderedRDD[Locus, Variant, BVector[Double]], r2Threshold: Double, window: Int) = {
//    val rangePartitioner = rdd.orderedPartitioner
//    val rangeBounds = rangePartitioner.rangeBounds
//    val partitionIndices = rdd.getPartitions.map(_.index)
//    val nPartitions = rdd.partitions.length
//
//    def computeDependencies(partitionId: Int): Array[Int] = {
//      if (partitionId == partitionIndices(0))
//        Array(partitionId)
//      else {
//        val startLocus = rangeBounds(partitionId - 1) // this is the best estimate of the first locus of the partition
//        val minimumLocus = Locus(startLocus.contig, math.max(startLocus.position - window, 0))
//        val partMinimum = rangePartitioner.getPartitionT(minimumLocus)
//        partitionIndices.filter(idx => idx >= partMinimum && idx <= partitionId).reverse
//      }
//    }
//
//    def pruneF = (x: Array[Iterator[(Variant, BVector[Double])]]) => {
//      val nPartitions = x.length
//      val targetIterator = x(0)
//      val prevPartitions = x.drop(1).reverse
//
//      if (nPartitions == 1)
//        targetIterator
//      else {
//        var targetData = targetIterator.toArray
//
//        prevPartitions.foreach { it =>
//          it.foreach { case (v2, sgs2) =>
//            targetData = targetData.filter { case (v, sgs) =>
//              if (v.contig != v2.contig || v.start - v2.start > window)
//                true
//              else {
//                val r = sgs.dot(sgs2)
//                (r * r: Double) < r2Threshold
//              }
//            }
//          }
//        }
//        targetData.iterator
//      }
//    }
//
//    def inputs(stage: Stage) = {
//      val partitionsToPrune = stage.prunePartitions.toSet
//
//      partitionIndices.map { i =>
//        if (stage.index == 0 || !partitionsToPrune.contains(i))
//          PartitionInput(Array(i), pruneF)
//        else {
//          PartitionInput(computeDependencies(i), pruneF)
//        }
//      }
//    }
//
//    val contigStartPartitions =
//      partitionIndices.filter { i =>
//        i == partitionIndices(0) || i == partitionIndices.last || rangeBounds(i).contig != rangeBounds(i - 1).contig
//      }
//
//    val contigPartitions = contigStartPartitions
//      .zipWithIndex
//      .map { case (p, i) =>
//        val startPartition = p
//        val endPartition = if (i != contigStartPartitions.length - 1) contigStartPartitions(i + 1) else contigStartPartitions(i)
//        partitionIndices.filter(idx => idx >= startPartition && idx <= endPartition)
//      }
//
//    val nStages = contigPartitions.map(p => p.length).max
//
//    val stages = for (stageNum <- 0 until nStages) yield
//      Stage(stageNum, contigPartitions.flatMap { p =>
//        if (stageNum < p.length)
//          Some(p(stageNum))
//        else
//          None
//      })
//
//    var newRDD = new GeneralRDD(rdd, inputs(stages(0))).persist(StorageLevel.MEMORY_AND_DISK)
//    newRDD.count()
//    rdd.unpersist()
//    var oldRDD = newRDD
//
//    for (j <- 1 until nStages) {
//      newRDD = new GeneralRDD(oldRDD, inputs(stages(j))).persist(StorageLevel.MEMORY_AND_DISK)
//      newRDD.count()
//      oldRDD.unpersist()
//      oldRDD = newRDD
//    }
//
//    newRDD
//  }

  def pruneGlobal2(rdd: OrderedRDD[Locus, Variant, BVector[Double]], r2Threshold: Double, window: Int) = {
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

    val prunedRDDs = mutable.HashMap.empty[Int, (RDD[(Variant, BVector[Double])], Int)]
    partitionIndices.foreach{ i => prunedRDDs(i) = (rdd, i)}

    val sc = rdd.sparkContext
    val testInputs = prunedRDDs.map{ case (pnum, (prunedRDD, i)) =>
      PartitionInput2[(Variant, BVector[Double])](pnum, Array(ParentInput(prunedRDD, i)), pruneF)
    }.toArray

    val testRDD = new GeneralRDD2(sc, testInputs)
    testRDD.count()



    for (pnum <- partitionIndices) {
//      println(s"partitionNumber=$pnum")
//      println(s"prunedRDDs=${prunedRDDs.mkString(",")}")
//      println(s"dependencies=${computeDependencies(pnum).mkString(",")}")
      val partitionInputs = Array(PartitionInput2[(Variant, BVector[Double])](
        pnum,
        computeDependencies(pnum).map{ i =>
          ParentInput(prunedRDDs(i)._1, prunedRDDs(i)._2)
        },
        pruneF)
      )
//      println(partitionInputs.map{pi => pi.parentInputs.mkString(",")}.mkString("\n"))
      val newRDD = new GeneralRDD2(sc, partitionInputs).persist(StorageLevel.MEMORY_AND_DISK)
      newRDD.count()
      prunedRDDs(pnum) = (newRDD, 0)
    }

//    println(s"prunedRDDfinal=${prunedRDDs.mkString(",")}")

    val partitionInputs = prunedRDDs.map{ case (pnum, (prunedRDD, i)) =>
      PartitionInput2[(Variant, BVector[Double])](pnum, Array(ParentInput(prunedRDD, i)), pruneF)
    }.toArray

    val newRDD = new GeneralRDD2(sc, partitionInputs)//.persist(StorageLevel.MEMORY_AND_DISK)
    rdd.unpersist()
    newRDD

//
//    val input = Array(PartitionInput2[(Variant, BVector[Double])](0, Array(ParentInput(rdd, 0)), pruneF))
//    var newRDD = new GeneralRDD2(rdd.sparkContext, input).persist(StorageLevel.MEMORY_AND_DISK)
//    newRDD.count()
////    rdd.unpersist()
//    latestRDDs += ((0, newRDD))
//
//    var oldRDD = newRDD
//
//    val input2 = Array(PartitionInput2[(Variant, BVector[Double])](1, computeDependencies(1).map{ i => ParentInput(latestRDDs.getOrElse(i, rdd), i)}, pruneF))
//
//    for (j <- 1 until nStages) {
//      newRDD = new GeneralRDD2(oldRDD, inputs(stages(j))).persist(StorageLevel.MEMORY_AND_DISK)
//      newRDD.count()
//      oldRDD.unpersist()
//      oldRDD = newRDD
//    }
//
//    newRDD
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
    val nVariantsInitial = vds.nVariants
    val nPartitionsInitial = vds.nPartitions

    info(s"InputData: nSamples=$nSamples nVariants=$nVariantsInitial nPartitions=$nPartitionsInitial")

    val minMemoryPerCore = math.ceil((1 / fractionMemoryToUse) * 8 * nSamples + variantByteOverhead)
    val (maxQueueSize, nPartitionsRequired) = estimateMemoryRequirements(nVariantsInitial, nSamples, memoryPerCore)

    info(s"minMemoryPerCore=${minMemoryPerCore / (1024 * 1024)}MB maxQueueSize=$maxQueueSize nPartitionsRequired=$nPartitionsRequired")

    if (localPruneThreshold < 0 || localPruneThreshold > 1)
      fatal(s"Local prune threshold must be in the range [0,1]. Found `$localPruneThreshold'.")

    if (r2Threshold < 0 || r2Threshold > 1)
      fatal(s"R^2 threshold must be in the range [0,1]. Found `$r2Threshold'.")

    if (window < 0)
      fatal(s"Window must be greater than or equal to 0. Found `$window'.")

    if (memoryPerCore < minMemoryPerCore)
      fatal(s"Memory per core must be greater than ${ minMemoryPerCore / (1024 * 1024) }MB")

    val standardizedRDD = vds.rdd.flatMapValues { case (va, gs) =>
      toNormalizedGtArray(gs, nSamples).map(BVector(_))
    }.asOrderedRDD

    var oldResult = LocalPruneResult(standardizedRDD, 0.0, 0, nVariantsInitial)

    var (newResult, duration) =
        time({
          val x = pruneLocal(oldResult, window, r2Threshold, localPruneThreshold, Option(maxQueueSize), coalesce = false)
          x.copy(rdd = x.rdd.repartition(nPartitionsRequired)(null).asOrderedRDD)
        })

    require(newResult.fractionPruned != 1.0)
    info(s"Local Prune ${ newResult.index }: fractionPruned = ${ newResult.fractionPruned } numVariantsRemaining = ${ newResult.numVariants } time=${ formatTime(duration) }")

    while (newResult.index == 1 || newResult.fractionPruned > localPruneThreshold) {
      println(s"while loop")
      oldResult = newResult
      val (result, duration) = time(pruneLocal(oldResult, window, r2Threshold, localPruneThreshold, None, coalesce = true))
      newResult = result
      info(s"Local Prune ${ newResult.index }: fractionPruned = ${ newResult.fractionPruned } numVariantsRemaining = ${ newResult.numVariants } time=${ formatTime(duration) }")
      require(newResult.fractionPruned != 1.0)
    }

    val (finalPrunedRDD, globalDuration) = time(pruneGlobal2(newResult.rdd.repartition(10)(null).asOrderedRDD, r2Threshold, window))
    info(s"Global Prune: numVariantsRemaining = ${ finalPrunedRDD.count() } time=${ formatTime(globalDuration) }")

    val annotatedRDD = finalPrunedRDD.mapValues(_ => Annotation(true))
    val schema = TStruct("prune" -> TBoolean)

    val result = vds.annotateVariants(annotatedRDD.orderedRepartitionBy(vds.rdd.orderedPartitioner), schema, dest)
    finalPrunedRDD.unpersist()
    result
  }
}