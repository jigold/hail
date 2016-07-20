package org.broadinstitute.hail.driver

import breeze.linalg.DenseVector
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.RichRDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr.BaseType
import org.broadinstitute.hail.methods.{GeneBurden, Phasing}
import org.broadinstitute.hail.variant.{Genotype, Variant, VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.utils.{SparseVariantSampleMatrix, SparseVariantSampleMatrixRRDBuilder}
import org.broadinstitute.hail.Utils._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}


/**
  * Created by laurent on 4/8/16.
  */
object RareVariantsBurden extends Command {

    def name = "rareBurden"

    def description = "Computes per-gene counts of rare variations for dominant and recessive modes. Recessive mode takes all MAF by default, so filtering should be done upstream or mPopRAF option used."

    def supportsMultiallelic = false

    def requiresVDS = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename prefix")
      var output: String = _
      @Args4jOption(required = false, name = "-mDAF", aliases = Array("--max-dominant-af"), usage = "Maximum allele frequency considered for dominant enrichment test")
      var mDAF : Double = 0.001
      @Args4jOption(required = false, name = "-mPopRAF", aliases = Array("--max-pop-recessive-af"), usage = "Maximum allele frequency in a single population (each SA strat) considered for recessive enrichment test. Not used if negative.")
      var mPopRAF : Double = -1.0
      @Args4jOption(required = false, name = "-mPopDAF", aliases = Array("--max-pop-dominant-af"), usage = "Maximum allele frequency in a single population (each SA strat) considered for dominant enrichment test. Not used if negative.")
      var mPopDAF : Double = -1.0
      @Args4jOption(required = false, name = "-vaStrat", aliases = Array("--va_stratification"), usage = "Stratify results based on variant annotations. Comma-separated list of annotations.")
      var vaStrat: String = ""
      @Args4jOption(required = false, name = "-saStrat", aliases = Array("--sa_stratification"), usage = "Stratify results based on sample annotations. Comma-separated list of annotations.")
      var saStrat: String = ""
      @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
      var number_partitions: Int = 1000
    }

    def newOptions = new Options

  object GeneBurdenResult {

    def getSingleVariantHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations ++
        Array("nHets","nHomVar","nDomNonRef")).mkString("\t")
    }

    def getVariantPairHeaderString(variantAnnotations: Array[String], sampleAnnotations: Array[String]) : String = {
      (sampleAnnotations ++
        variantAnnotations.map(ann => ann + "1") ++
        variantAnnotations.map(ann => ann + "2") ++
        Array("nCompHets")).mkString("\t")
    }

    /**private def zipVA(va1: Array[String],va2: Array[String]) : Array[String] = {
      * va1.zip(va2).map({case(a1,a2) => a1+"/"+a2})
      * }**/

  }

    class GeneBurdenResult(svsm: SparseVariantSampleMatrix, val vaStrat: Array[String], val saStrat: Array[String], val uniqueSaStrat: Array[String], val samplesStrat: IndexedSeq[Int], val mDAF: Double, val mPopDAF: Double){

      //Caches variant information
      private val vaCache = mutable.Map[Int,String]()
      private val phaseCache = mutable.Map[(String,String),Option[Double]]()

      //Stores results
      //Stores nHet, nHomVar, nDomVars
      private val singleVariantsCounts = mutable.Map[String,(Int,Int,Int)]()
      //Stores the number of coumpound hets
      private val compoundHetcounts = mutable.Map[String,Int]()

      //Get annotation queriers
      var vaQueriers = Array.ofDim[(BaseType,Querier)](vaStrat.size)
      vaStrat.indices.foreach({ i =>
        vaQueriers(i) = svsm.queryVA(vaStrat(i))
      })

      var saQueriers = Array.ofDim[(BaseType,Querier)](saStrat.size)
      saStrat.indices.foreach({ i =>
        saQueriers(i) = svsm.querySA(saStrat(i))
      })

      //Compute the MAFs
      private val nSAStrats = uniqueSaStrat.size
      private val nSamplesPerStrat = samplesStrat.foldLeft(Array.fill(nSAStrats)(0))({(ag,s) => ag(s)+=1; ag})
      private val maf = new Array[Double](svsm.variants.size)
      private val maxPopMaf = new Array[Double](svsm.variants.size)

      if (mPopDAF >= 0) {
        computeMaxPopMAF()
      }else{
        svsm.foreachVariant({
          case(v,vi,si,gt) =>
            var noCall = 0
            var ac = 0
            gt.foreach({ g => if( g > -1) ac+= g else noCall +=1})
            maf(vi) = ac / (2* (svsm.nSamples - noCall))
        })
      }

      //Compute and record the pre-sample counts
      computePerSampleCounts()

      //Compute MAF per population (and overall)
      private def computeMaxPopMAF() : Unit = {
        svsm.foreachVariant({
          (v, vi, si, gt) =>
            val nNoCall = Array.fill(nSAStrats)(0)
            val ac = Array.fill(nSAStrats)(0)
              si.indices.foreach({
                i =>
                  if (gt(i) > -1) {
                    ac(samplesStrat(si(i))) += gt(i)
                  } else {
                    nNoCall(samplesStrat(si(i))) += 1
                  }
              })
              maxPopMaf(vi) = nSamplesPerStrat.indices.map(i => if (nSamplesPerStrat(i) - nNoCall(i) == 0) 0.0 else ac(i).toDouble / (2 * (nSamplesPerStrat(i) - nNoCall(i)))).max
              maf(vi) = ac.sum.toDouble / (2 * (svsm.nSamples - nNoCall.sum))

        })
      }

      private def computePerSampleCounts() : Unit = {

        //Private class to store sample-level information
        class sampleVariants{
          //var homRefVars = mutable.HashSet[String]()
          var hetVars = mutable.HashSet[String]()
          var homVarVars = mutable.HashSet[String]()
          //var NoCallVars = mutable.HashSet[String]()
          var compHetVars = mutable.HashSet[String]()
          var domVars = mutable.HashSet[String]()
        }

        val samplesCounts = new Array[sampleVariants](svsm.sampleIDs.size)

        //Creates the sets of het variants within a sample to compute compound hets
        val s_hetVariantBuilder = new ArrayBuilder.ofInt()
        s_hetVariantBuilder.sizeHint(svsm.variants.size) //TODO: Remove then updating to Scala 2.12+

        svsm.foreachSample({
          (s, si, variants, genotypes) =>
            //Get the sample stratification
            val saStrat = uniqueSaStrat(samplesStrat(si))

            //Save sampleCounts for this sample
            val sampleCounts = new sampleVariants()

            //Compute results for single variants
            s_hetVariantBuilder.clear()
            variants.indices.foreach({
              i =>
                val vi = variants(i)
                var addHet = 0
                var addHomVar = 0
                var addDomVar = 0
                val va = concatStrats(saStrat,getVAStrats(vi))

                genotypes(i) match{
                  //case -1 => sampleCounts.NoCallVars.add(concatStrats(saStrat,getVAStrats(vi)))
                  case 1 =>
                    if(sampleCounts.hetVars.add(va)){ addHet += 1 }
                    s_hetVariantBuilder += vi
                    if(maf(vi) < mDAF && (mPopDAF < 0 || maxPopMaf(vi) < mPopDAF)){
                      if(sampleCounts.domVars.add(va)){ addDomVar +=1 }
                    }
                  case 2 =>
                    if(sampleCounts.homVarVars.add(va)){ addHomVar += 1 }
                    if(maf(vi) < mDAF && (mPopDAF < 0 || maxPopMaf(vi) < mPopDAF)){
                      if(sampleCounts.domVars.add(va)){ addDomVar +=1 }
                    }
                  case _ =>
                }

                if(addHet + addHomVar + addDomVar > 0){
                  singleVariantsCounts.get(va) match {
                    case Some((nHets, nHomVars, nDomVars)) => singleVariantsCounts(va) = (nHets+addHet,nHomVars+addHomVar,nDomVars+addDomVar)
                    case None => singleVariantsCounts(va) = (addHet,addHomVar,addDomVar)
                  }
                }

            })

            //Computes compound het results based on all pairs of het variants
            val s_variants = s_hetVariantBuilder.result()
            s_variants.indices.foreach({
              i1 =>
                val v1i = s_variants(i1)
                Range(i1+1,s_variants.size).foreach({
                  i2 =>
                    val v2i = s_variants(i2)
                    getPhase(svsm.variants(v1i),svsm.variants(v2i)) match {
                      case Some(sameHap) =>
                        if(sameHap < 0.5) {
                          //Sort annotations before insertion to prevent duplicates
                          val va1 = getVAStrats(v1i)
                          val va2 = getVAStrats(v2i)
                          val compHetVA = if(va1 < va2) concatStrats(saStrat,getVAStrats(v1i), getVAStrats(v2i)) else concatStrats(saStrat,getVAStrats(v2i), getVAStrats(v1i))
                          if(sampleCounts.compHetVars.add(compHetVA)){
                            compoundHetcounts.get(compHetVA) match {
                              case Some(nCompHets) => compoundHetcounts(compHetVA) = nCompHets+1
                              case None => compoundHetcounts(compHetVA) = 1
                            }
                          }
                        }
                      case None =>
                    }

                })
            })
        })

      }

      private def concatStrats(strats: String*): String ={
        strats.filter({x => !x.isEmpty}).mkString("\t")
      }

      def getSingleVariantsStats(group_name : String) : String = {
        val str = singleVariantsCounts.map({case (ann,(nHets,nHoms,nDoms)) =>
          "%s%s\t%d\t%d\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nHets,nHoms,nDoms)
        }).mkString("\n")
        if(str.isEmpty){
          group_name + "\t" + Array.fill(3 + saStrat.size + vaStrat.size)("NA").mkString("\t")
        }else{
          str
        }
      }

      def getVariantPairsStats(group_name : String) : String = {
        val str = compoundHetcounts.map({case (ann,nCHets) =>
          "%s%s\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nCHets)
        }).mkString("\n")
        if(str.isEmpty){
          group_name + "\t" + Array.fill(1 + saStrat.size + 2*vaStrat.size)("NA").mkString("\t")
        }else{
          str
        }
      }

      private def getVAStrats(variantIndex : Int) :  String = {
        vaCache.get(variantIndex) match {
          case Some(ann) => ann
          case None =>
            val va =vaQueriers.map({querier =>
              svsm.getVariantAnnotation(variantIndex,querier._2).getOrElse("NA").toString()
            }).mkString("\t")
            //addVAValues(va)
            vaCache(variantIndex) = va
            va
        }
      }

      private def getPhase(v1: String, v2: String) : Option[Double] = {
        phaseCache.get((v1,v2)) match{
          case Some(p) => p
          case None =>
            val pPhase = Phasing.probOnSameHaplotypeWithEM(svsm.getGenotypeCounts(v1,v2))
            phaseCache.update((v1,v2),pPhase)
            pPhase
        }
      }
    }


    def run(state: State, options: Options): State = {

      //Get sample and variant stratifications
      val saStrats = if(!options.saStrat.isEmpty()) options.saStrat.split(",") else Array[String]()
      val vaStrats = if(!options.vaStrat.isEmpty()) options.vaStrat.split(",") else Array[String]()

      val partitioner = new HashPartitioner(options.number_partitions)

      //Get annotations
      val geneAnn = state.vds.queryVA(options.gene_annotation)._2

      //Get annotation queriers
      val saQueriers = for (strat <- saStrats) yield {
        state.vds.querySA(strat)
      }

      //Group samples by stratification
      val uniqueSaStrats = new ArrayBuffer[String]()
      val samplesStrat = state.sc.broadcast(for (i <- state.vds.sampleIds.indices) yield {
        val strat = saQueriers.map({case(basetype,querier) => querier(state.vds.sampleAnnotations(i)).getOrElse("NA")}).mkString("\t")
        val stratIndex = uniqueSaStrats.indexOf(strat)
        if(stratIndex < 0){
          uniqueSaStrats += strat
          uniqueSaStrats.size - 1
        }else{
          stratIndex
        }
      })
      val uniqueSaStratsBr = state.sc.broadcast(uniqueSaStrats.toArray)

      val mDAF = options.mDAF
      val mPopRAF = options.mPopRAF
      val mPopDAF = options.mPopDAF

      //Sanity check when using PopAF
      //This is needed as no AF-check is done for the recessive model when aggregating variants
      //Could be changed if needed...
      if(mPopDAF > mPopRAF){
        fatal("The maximum AF for the dominant model cannot be larger than the maximum AF for the recessive model")
      }

      //Filter variants that have a MAF higher than what we're looking for in ANY stratification
      val maxPopAF = if(options.mPopRAF > options.mPopDAF) options.mPopRAF else options.mPopDAF

      //Filter variants
      def popAFVariantFilter = {(v: Variant, va: Annotation, gs: Iterable[Genotype]) =>

        //Get AN and AC for each of the strats
        val an = Array.fill(uniqueSaStratsBr.value.size)(0)
        val ac = Array.fill(uniqueSaStratsBr.value.size)(0)

        gs.zipWithIndex.foreach({ case (genotype, i) =>
          if (genotype.isCalled) {
            an(samplesStrat.value(i)) += 2
            ac(samplesStrat.value(i)) += genotype.nNonRefAlleles.getOrElse(0)
          }
        })

        //Check that the max population AF isn't above the threshold
        val popAF = uniqueSaStratsBr.value.indices.foldLeft(0.0) {
          (ag, sti) =>
            val af = if (an(sti) > 0) ac(sti).toDouble / an(sti) else 0.0
            if (af > ag) af else ag
        }

        //If the dominant population AF isn't used and variant would be filtered via pop AF filter,
        //make sure it would also be filtered with the overall dominant filter
        if(popAF >= maxPopAF && mPopDAF < 0){
            ac.sum.toDouble / an.sum < mDAF
        }else {
          popAF < maxPopAF
        }
      }

      val filteredVDS = if(mPopRAF < 0) state.vds else state.vds.filterVariants(popAFVariantFilter)

      val gb = SparseVariantSampleMatrixRRDBuilder.buildByVAstoreVAandSA(
        filteredVDS,
        state.sc,
        partitioner,
        vaStrats,
        saStrats
      )({case (v,va) => geneAnn(va).get.toString}).mapValues(
        {case svsm => new GeneBurdenResult(svsm, vaStrats, saStrats, uniqueSaStratsBr.value, samplesStrat.value, mDAF, mPopDAF)}
      ).persist(StorageLevel.MEMORY_AND_DISK)

      //Write out single variant stats
      new RichRDD(gb.map(
        {case (gene,gr) => gr.getSingleVariantsStats(gene)}
      )).writeTable(options.output +".single.txt",
        Some("gene\t" + GeneBurdenResult.getSingleVariantHeaderString(vaStrats,saStrats)))

      //Write out pair variants stats
      new RichRDD(gb.map(
        {case (gene,gr) => gr.getVariantPairsStats(gene)}
      )).writeTable(options.output +".pair.txt",
        Some("gene\t" + GeneBurdenResult.getVariantPairHeaderString(vaStrats,saStrats)))

      state
    }
}
