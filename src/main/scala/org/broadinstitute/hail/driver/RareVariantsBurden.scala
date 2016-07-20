package org.broadinstitute.hail.driver

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

    def description = "Computes per-gene counts of rare variations for dominant and recessive modes"

    def supportsMultiallelic = false

    def requiresVDS = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename prefix")
      var output: String = _
      @Args4jOption(required = false, name = "-mraf", aliases = Array("--max-recessive-af"), usage = "Maximum allele frequency considered for recessive enrichment test")
      var mraf : Double = 0.01
      @Args4jOption(required = false, name = "-mdaf", aliases = Array("--max-dominant-af"), usage = "Maximum allele frequency considered for dominant enrichment test")
      var mdaf : Double = 0.001
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
        Array("nHets","nHomVar")).mkString("\t")
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

    class GeneBurdenResult(svsm: SparseVariantSampleMatrix, val vaStrat: Array[String], val saStrat: Array[String]){

      //Caches variant information
      private val vaCache = mutable.Map[Int,String]()
      private val phaseCache = mutable.Map[(String,String),Option[Double]]()

      //Stores results
      //Stores nHet, nHomVar
      private val singleVariantsCounts = mutable.Map[String,(Int,Int)]()
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


      computePerSampleCounts()


      def computePerSampleCounts() : Unit = {

        //Private class to store sample-level information
        class sampleVariants{
          //var homRefVars = mutable.HashSet[String]()
          var hetVars = mutable.HashSet[String]()
          var homVarVars = mutable.HashSet[String]()
          //var NoCallVars = mutable.HashSet[String]()
          var compHetVars = mutable.HashSet[String]()
        }

        val samplesCounts = new Array[sampleVariants](svsm.sampleIDs.size)

        //Creates the sets of het variants within a sample to compute compound hets
        val s_hetVariantBuilder = new ArrayBuilder.ofInt()
        s_hetVariantBuilder.sizeHint(svsm.variants.size) //TODO: Remove then updating to Scala 2.12+

        svsm.foreachSample({
          (sid, variants, genotypes) =>
            //Get the sample stratification
            val saStrat = saQueriers.map({querier =>
              svsm.getSampleAnnotation(sid,querier._2).getOrElse("NA").toString()
            }).mkString("\t")

            //Save sampleCounts for this sample
            val sampleCounts = new sampleVariants()

            //Get all variants with het/homvar genotype
            s_hetVariantBuilder.clear()
            variants.indices.foreach({
              i =>
                val vi = variants(i)
                genotypes(i) match{
                  //case -1 => sampleCounts.NoCallVars.add(concatStrats(saStrat,getVAStrats(vi)))
                  case 1 =>
                    sampleCounts.hetVars.add(concatStrats(saStrat,getVAStrats(vi)))
                    s_hetVariantBuilder += vi
                  case 2 => sampleCounts.homVarVars.add(concatStrats(saStrat,getVAStrats(vi)))
                  case _ =>
                }
            })

            //Computes all pairs of variants
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
                          if (va1 < va2) {
                            sampleCounts.compHetVars.add(concatStrats(saStrat,getVAStrats(v1i), getVAStrats(v2i)))
                          }
                          else {
                            sampleCounts.compHetVars.add(concatStrats(saStrat,getVAStrats(v2i), getVAStrats(v1i)))
                          }
                        }
                      case None =>
                    }

                })
            })

            //Add sample results to global results
            sampleCounts.hetVars.foreach({
              ann => singleVariantsCounts.get(ann) match {
                case Some((nHets, nHomVars)) => singleVariantsCounts(ann) = (nHets+1,nHomVars)
                case None => singleVariantsCounts(ann) = (1,0)
              }
            })
            sampleCounts.homVarVars.foreach({
              ann => singleVariantsCounts.get(ann) match {
                case Some((nHets, nHomVars)) => singleVariantsCounts(ann) = (nHets,nHomVars+1)
                case None => singleVariantsCounts(ann) = (0,1)
              }
            })
            sampleCounts.compHetVars.foreach({
              ann => compoundHetcounts.get(ann) match {
                case Some(nCompHets) => compoundHetcounts(ann) = nCompHets+1
                case None => compoundHetcounts(ann) = 1
              }
            })

        })

      }

      private def concatStrats(strats: String*): String ={
        strats.filter({x => !x.isEmpty}).mkString("\t")
      }

      def getSingleVariantsStats(group_name : String) : String = {
        singleVariantsCounts.map({case (ann,(nHets,nHoms)) =>
          "%s%s\t%d\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nHets,nHoms)
        }).mkString("\n")
      }

      def getVariantPairsStats(group_name : String) : String = {
        compoundHetcounts.map({case (ann,nCHets) =>
          "%s%s\t%d".format(group_name,if(ann.isEmpty) "" else "\t"+ann,nCHets)
        }).mkString("\n")
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

      //Get annotation queriers
      val saQueriers = for (strat <- saStrats) yield {
        state.vds.querySA(strat)
      }

      //Group samples by stratification
      val samplesByStrat = state.sc.broadcast(for (i <- state.vds.sampleIds.indices) yield {
        saQueriers.map({case(basetype,querier) => querier(state.vds.sampleAnnotations(i))}).mkString("\t")
      })

      //Unique sample strats
      val uniqueSaStrats = state.sc.broadcast(samplesByStrat.value.toSet)

      //Filter variants that have a MAF higher than what we're looking for in ALL stratifications
      val maxAF = if(options.mraf > options.mdaf) options.mraf else options.mdaf
      def rareVariantsFilter = {(v: Variant, va: Annotation, gs: Iterable[Genotype]) =>

        //Get AN and AC for each of the strats
        val an = uniqueSaStrats.value.foldLeft(mutable.Map[String,Int]()){case(m, strat) => m.update(strat,0); m}
        val ac = uniqueSaStrats.value.foldLeft(mutable.Map[String,Int]()){case(m, strat) => m.update(strat,0); m}

        gs.zipWithIndex.foreach({ case (genotype, i) =>
          if (genotype.isHomRef) {
            an(samplesByStrat.value(i)) += 2
          }
          else if(genotype.isHet){
            an(samplesByStrat.value(i)) += 2
            ac(samplesByStrat.value(i)) += 1
          }
          else if(genotype.isHomVar){
            an(samplesByStrat.value(i)) += 2
            ac(samplesByStrat.value(i)) += 2
          }
        })

        //Check that the max AF isn't above the threshold
        uniqueSaStrats.value.foldLeft(0) {
          (ag, st) =>
            val af = if (an(st) > 0) ac(st) / an(st) else 0
            if (af > ag) af else ag
        } < maxAF

      }

      val partitioner = new HashPartitioner(options.number_partitions)

      //Get annotations
      val geneAnn = state.vds.queryVA(options.gene_annotation)._2

      info("Computing gene burden")

      val gb = SparseVariantSampleMatrixRRDBuilder.buildByVAstoreVAandSA(
        state.vds.filterVariants(rareVariantsFilter),
        state.sc,
        partitioner,
        vaStrats,
        saStrats
      )({case (v,va) => geneAnn(va).get.toString}).mapValues(
        {case svsm => new GeneBurdenResult(svsm, vaStrats, saStrats)}
      ).persist(StorageLevel.MEMORY_AND_DISK)

      info("Writing out results")

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
