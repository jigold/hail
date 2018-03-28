package is.hail.io.annotators

import is.hail.HailContext
import is.hail.annotations.Annotation
import is.hail.expr.types._
import is.hail.io.vcf.LoadVCF
import is.hail.table.Table
import is.hail.utils.{Interval, _}
import is.hail.variant._
import org.apache.spark.sql.Row

object BedAnnotator {
  def apply(hc: HailContext, filename: String, rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    skipInvalidIntervals: Boolean = false): Table = {
    // this annotator reads files in the UCSC BED spec defined here: https://genome.ucsc.edu/FAQ/FAQformat.html#format1

    val hasTarget = hc.hadoopConf.readLines(filename) { lines =>

      val (header, remainder) = lines.span(line =>
        line.value.startsWith("browser") ||
          line.value.startsWith("track") ||
          line.value.matches("""^\w+=("[\w\d ]+"|\d+).*"""))

      if (remainder.isEmpty)
        fatal("bed file contains no non-header lines")

      remainder.next()
        .map { l =>
          val length = l.split("""\s+""")
            .length

          if (length >= 4)
            true
          else if (length == 3)
            false
          else
            fatal(s"too few fields for BED file: expected 3 or more, but found $length")
        }.value
    }

    val expectedLength = if (hasTarget) 4 else 3

    val locusSchema = TLocus.schemaFromRG(rg)

    val schema = if (hasTarget)
      TStruct("interval" -> TInterval(locusSchema), "target" -> TString())
    else
      TStruct("interval" -> TInterval(locusSchema))

    implicit val ord = TInterval(locusSchema).ordering

    val rdd = hc.sc.textFileLines(filename)
      .filter(line =>
        !(line.value.startsWith("browser") ||
          line.value.startsWith("track") ||
          line.value.matches("""^\w+=("[\w\d ]+"|\d+).*""") ||
          line.value.isEmpty))
      .flatMap {
        _.map { line =>
          val spl = line.split("""\s+""")
          if (spl.length < expectedLength)
            fatal(s"Expected at least $expectedLength fields, but found ${ spl.length }")
          val chrom = spl(0)
          // transform BED 0-based coordinates to Hail/VCF 1-based coordinates
          val start = spl(1).toInt + 1
          val end = spl(2).toInt

          if (skipInvalidIntervals && rg.forall(rg => !rg.isValidContig(chrom) || !rg.isValidLocus(chrom, start) || !rg.isValidLocus(chrom, end))) {
            warn(s"Interval is not consistent with reference genome '${ rg.get.name }': '$line'.")
            None
          } else {
            val interval = Interval(Locus.annotation(chrom, start, rg), Locus.annotation(chrom, end, rg), true, true)

            if (hasTarget)
              Some(Row(interval, spl(3)))
            else
              Some(Row(interval))
          }
        }.value
      }

    Table(hc, rdd, schema, Array("interval"))
  }
}
