package is.hail.variant

import java.io.InputStream

import is.hail.HailContext
import is.hail.check.Gen
import is.hail.expr.{JSONExtractGenomeReference, TInterval, TLocus, TVariant}
import is.hail.utils._
import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

abstract class GRBase extends Serializable {
  val variant: TVariant = TVariant(this)
  val locus: TLocus = TLocus(this)
  val interval: TInterval = TInterval(this)

  def name: String

  def variantOrdering: Ordering[Variant]

  def locusOrdering: Ordering[Locus]

  def intervalOrdering: Ordering[Interval[Locus]]

  def isValidContig(contig: String): Boolean

  def isValidPosition(contig: String, start: Int): Boolean

  def checkVariant(v: Variant): Unit

  def checkVariant(contig: String, pos: Int, ref: String, alts: Array[String]): Unit

  def checkVariant(contig: String, start: Int, ref: String, alts: java.util.ArrayList[String]): Unit

  def checkVariant(contig: String, pos: Int, ref: String, alt: String): Unit

  def checkLocus(l: Locus): Unit

  def checkLocus(contig: String, pos: Int): Unit

  def checkInterval(i: Interval[Locus]): Unit

  def checkInterval(l1: Locus, l2: Locus): Unit

  def checkInterval(contig: String, start: Int, end: Int): Unit

  def contigLength(contig: String): Int

  def contigLength(contigIdx: Int): Int

  def inX(contigIdx: Int): Boolean

  def inX(contig: String): Boolean

  def inY(contigIdx: Int): Boolean

  def inY(contig: String): Boolean

  def isMitochondrial(contigIdx: Int): Boolean

  def isMitochondrial(contig: String): Boolean

  def inXPar(locus: Locus): Boolean

  def inYPar(locus: Locus): Boolean

  def compare(c1: String, c2: String): Int

  def compare(v1: IVariant, v2: IVariant): Int

  def compare(l1: Locus, l2: Locus): Int

  def toJSON: JValue

  def unify(concrete: GRBase): Boolean

  def isBound: Boolean

  def clear(): Unit

  def subst(): GRBase
}

case class GenomeReference(name: String, contigs: Array[String], lengths: Map[String, Int],
  xContigs: Set[String] = Set.empty[String], yContigs: Set[String] = Set.empty[String],
  mtContigs: Set[String] = Set.empty[String], parInput: Array[(Locus, Locus)] = Array.empty[(Locus, Locus)]) extends GRBase {

  val nContigs = contigs.length

  if (nContigs <= 0)
    fatal("Must have at least one contig in the reference genome.")

  if (!contigs.areDistinct())
    fatal("Repeated contig names are not allowed.")

  val missingLengths = contigs.toSet.diff(lengths.keySet)
  val extraLengths = lengths.keySet.diff(contigs.toSet)

  if (missingLengths.nonEmpty)
    fatal(s"No lengths given for the following contigs: ${ missingLengths.mkString(", ")}")

  if (extraLengths.nonEmpty)
    fatal(s"Contigs found in `lengths' that are not present in `contigs': ${ extraLengths.mkString(", ")}")

  if (xContigs.intersect(yContigs).nonEmpty)
    fatal(s"Found the contigs `${ xContigs.intersect(yContigs).mkString(", ") }' in both X and Y contigs.")

  if (xContigs.intersect(mtContigs).nonEmpty)
    fatal(s"Found the contigs `${ xContigs.intersect(mtContigs).mkString(", ") }' in both X and MT contigs.")

  if (yContigs.intersect(mtContigs).nonEmpty)
    fatal(s"Found the contigs `${ yContigs.intersect(mtContigs).mkString(", ") }' in both Y and MT contigs.")

  val contigsIndex: Map[String, Int] = contigs.zipWithIndex.toMap
  val contigsSet: Set[String] = contigs.toSet
  val lengthsByIndex: Array[Int] = contigs.map(lengths)

  lengths.foreach { case (n, l) =>
    if (l <= 0)
      fatal(s"Contig length must be positive. Contig `$n' has length equal to $l.")
  }

  val xNotInRef = xContigs.diff(contigsSet)
  val yNotInRef = yContigs.diff(contigsSet)
  val mtNotInRef = mtContigs.diff(contigsSet)

  if (xNotInRef.nonEmpty)
    fatal(s"The following X contig names are absent from the reference: `${ xNotInRef.mkString(", ") }'.")

  if (yNotInRef.nonEmpty)
    fatal(s"The following Y contig names are absent from the reference: `${ yNotInRef.mkString(", ") }'.")

  if (mtNotInRef.nonEmpty)
    fatal(s"The following mitochondrial contig names are absent from the reference: `${ mtNotInRef.mkString(", ") }'.")

  val xContigIndices = xContigs.map(contigsIndex)
  val yContigIndices = yContigs.map(contigsIndex)
  val mtContigIndices = mtContigs.map(contigsIndex)

  val variantOrdering = new Ordering[Variant] {
    def compare(x: Variant, y: Variant): Int = GenomeReference.compare(contigsIndex, x, y)
  }

  val locusOrdering = new Ordering[Locus] {
    def compare(x: Locus, y: Locus): Int = GenomeReference.compare(contigsIndex, x, y)
  }

  val intervalOrdering = new Ordering[Interval[Locus]] {
    def compare(x: Interval[Locus], y: Interval[Locus]): Int = GenomeReference.compare(contigsIndex, x, y)
  }

  val par = parInput.map { case (start, end) =>
    if (start.contig != end.contig)
      fatal(s"The contigs for the `start' and `end' of a PAR interval must be the same. Found `$start-$end'.")

    if ((!xContigs.contains(start.contig) && !yContigs.contains(start.contig)) ||
      (!xContigs.contains(end.contig) && !yContigs.contains(end.contig)))
      fatal(s"The contig name for PAR interval `$start-$end' was not found in xContigs `${ xContigs.mkString(",") }' or in yContigs `${ yContigs.mkString(",") }'.")

    Interval(start, end)(locusOrdering)
  }

  def contigLength(contig: String): Int = lengths.get(contig) match {
    case Some(l) => l
    case None => fatal(s"Invalid contig name: `$contig'.")
  }

  def contigLength(contigIdx: Int): Int = {
    require(contigIdx >= 0 && contigIdx < nContigs)
    lengthsByIndex(contigIdx)
  }

  def isValidContig(contig: String): Boolean = contigsSet.contains(contig)

  def isValidPosition(contig: String, pos: Int): Boolean = pos > 0 && pos <= contigLength(contig)

  def checkLocus(l: Locus): Unit = checkLocus(l.contig, l.position)

  def checkLocus(contig: String, pos: Int): Unit = {
    if (!isValidContig(contig))
      fatal(s"Invalid locus `$contig:$pos' found. Contig `$contig' is not in the reference genome `$name'.")
    if (!isValidPosition(contig, pos))
      fatal(s"Invalid locus `$contig:$pos' found. Position `$pos' is not within the range [1-${ contigLength(contig) }] for reference genome `$name'.")
  }

  def checkVariant(v: Variant): Unit = {
    if (!isValidContig(v.contig))
      fatal(s"Invalid variant `$v' found. Contig `${ v.contig }' is not in the reference genome `$name'.")
    if (!isValidPosition(v.contig, v.start))
      fatal(s"Invalid variant `$v' found. Start `${ v.start }' is not within the range [1-${ contigLength(v.contig) }] for reference genome `$name'.")
  }

  def checkVariant(contig: String, start: Int, ref: String, alt: String): Unit = {
    val v = s"$contig:$start:$ref:$alt"
    if (!isValidContig(contig))
      fatal(s"Invalid variant `$v' found. Contig `$contig' is not in the reference genome `$name'.")
    if (!isValidPosition(contig, start))
      fatal(s"Invalid variant `$v' found. Start `$start' is not within the range [1-${ contigLength(contig) }] for reference genome `$name'.")
  }

  def checkVariant(contig: String, start: Int, ref: String, alts: Array[String]): Unit = checkVariant(contig, start, ref, alts.mkString(","))

  def checkVariant(contig: String, start: Int, ref: String, alts: java.util.ArrayList[String]): Unit = checkVariant(contig, start, ref, alts.asScala.toArray)

  def checkInterval(i: Interval[Locus]): Unit = {
    if (!isValidContig(i.start.contig))
      fatal(s"Invalid interval `$i' found. Contig `${ i.start.contig }' is not in the reference genome `$name'.")
    if (!isValidContig(i.end.contig))
      fatal(s"Invalid interval `$i' found. Contig `${ i.end.contig }' is not in the reference genome `$name'.")
    if (!isValidPosition(i.start.contig, i.start.position))
      fatal(s"Invalid interval `$i' found. Start `${ i.start }' is not within the range [1-${ contigLength(i.start.contig) }] for reference genome `$name'.")
    if (!isValidPosition(i.end.contig, i.end.position))
      fatal(s"Invalid interval `$i' found. End `${ i.end }' is not within the range [1-${ contigLength(i.end.contig) }] for reference genome `$name'.")
  }

  def checkInterval(l1: Locus, l2: Locus): Unit = {
    val i = s"$l1-$l2"
    if (!isValidPosition(l1.contig, l1.position))
      fatal(s"Invalid interval `$i' found. Locus `$l1' is not in the reference genome `$name'.")
    if (!isValidPosition(l2.contig, l2.position))
      fatal(s"Invalid interval `$i' found. Locus `$l2' is not in the reference genome `$name'.")
  }

  def checkInterval(contig: String, start: Int, end: Int): Unit = {
    val i = s"$contig:$start-$end"
    if (!isValidContig(contig))
      fatal(s"Invalid interval `$i' found. Contig `$contig' is not in the reference genome `$name'.")
    if (!isValidPosition(contig, start))
      fatal(s"Invalid interval `$i' found. Start `$start' is not within the range [1-${ contigLength(contig) }] for reference genome `$name'.")
    if (!isValidPosition(contig, end))
      fatal(s"Invalid interval `$i' found. End `$end' is not within the range [1-${ contigLength(contig) }] for reference genome `$name'.")
  }

  def inX(contigIdx: Int): Boolean = xContigIndices.contains(contigIdx)

  def inX(contig: String): Boolean = xContigs.contains(contig)

  def inY(contigIdx: Int): Boolean = yContigIndices.contains(contigIdx)

  def inY(contig: String): Boolean = yContigs.contains(contig)

  def isMitochondrial(contigIdx: Int): Boolean = mtContigIndices.contains(contigIdx)

  def isMitochondrial(contig: String): Boolean = mtContigs.contains(contig)

  def inXPar(l: Locus): Boolean = inX(l.contig) && par.exists(_.contains(l))

  def inYPar(l: Locus): Boolean = inY(l.contig) && par.exists(_.contains(l))

  def compare(contig1: String, contig2: String): Int = GenomeReference.compare(contigsIndex, contig1, contig2)

  def compare(v1: IVariant, v2: IVariant): Int = GenomeReference.compare(contigsIndex, v1, v2)

  def compare(l1: Locus, l2: Locus): Int = GenomeReference.compare(contigsIndex, l1, l2)

  def compare(i1: Interval[Locus], i2: Interval[Locus]): Int = GenomeReference.compare(contigsIndex, i1, i2)

  def toJSON: JValue = JObject(
    ("name", JString(name)),
    ("contigs", JArray(contigs.map(c => JObject(("name", JString(c)), ("length", JInt(lengths(c))))).toList)),
    ("xContigs", JArray(xContigs.map(JString(_)).toList)),
    ("yContigs", JArray(yContigs.map(JString(_)).toList)),
    ("mtContigs", JArray(mtContigs.map(JString(_)).toList)),
    ("par", JArray(par.map(_.toJSON(_.toJSON)).toList))
  )

  def validateContigRemap(contigMapping: Map[String, String]) {
    val badContigs = mutable.Set[(String, String)]()

    contigMapping.foreach { case (oldName, newName) =>
      if (!contigsSet.contains(newName))
        badContigs += ((oldName, newName))
    }

    if (badContigs.nonEmpty)
      fatal(s"Found ${ badContigs.size } ${ plural(badContigs.size, "contig mapping") } that do not have remapped contigs in the reference genome `$name':\n  " +
        s"@1", contigMapping.truncatable("\n  "))
  }

  override def equals(other: Any): Boolean = {
    other match {
      case gr: GenomeReference =>
        name == gr.name &&
          contigs.sameElements(gr.contigs) &&
          lengths == gr.lengths &&
          xContigs == gr.xContigs &&
          yContigs == gr.yContigs &&
          mtContigs == gr.mtContigs &&
          par.sameElements(gr.par)
      case _ => false
    }
  }

  def unify(concrete: GRBase): Boolean = this eq concrete

  def isBound: Boolean = true

  def clear() {}

  def subst(): GenomeReference = this

  override def toString: String = name
}

object GenomeReference {
  var references: Map[String, GenomeReference] = Map()
  val GRCh37: GenomeReference = fromResource("reference/grch37.json")
  val GRCh38: GenomeReference = fromResource("reference/grch38.json")
  var defaultReference = GRCh37

  def addReference(gr: GenomeReference) {
    if (references.contains(gr.name))
      fatal(s"Cannot add reference genome. Reference genome `${ gr.name }' already exists.")
    references += (gr.name -> gr)
  }

  def getReference(name: String): GenomeReference = {
    references.get(name) match {
      case Some(gr) => gr
      case None => fatal(s"No genome reference with name `$name' exists. Available references: `${ references.keys.mkString(", ") }'.")
    }
  }

  def hasReference(name: String): Boolean = references.contains(name)

  def deleteReference(name: String): Unit = {
    if (!hasReference(name))
      fatal(s"Cannot delete reference genome. Reference genome `$name' does not exist.")
    references -= name
  }

  def setDefaultReference(gr: GenomeReference) {
    assert(references.contains(gr.name))
    defaultReference = gr
  }

  def setDefaultReference(hc: HailContext, grSource: String) {
    defaultReference =
      if (hasReference(grSource))
        getReference(grSource)
      else
        fromFile(hc, grSource)
  }

  def fromJSON(json: JValue): GenomeReference = json.extract[JSONExtractGenomeReference].toGenomeReference

  def fromResource(file: String): GenomeReference = {
    val gr = loadFromResource[GenomeReference](file) {
      (is: InputStream) => fromJSON(JsonMethods.parse(is))
    }
    addReference(gr)
    gr
  }

  def fromFile(hc: HailContext, file: String): GenomeReference = {
    val gr = hc.hadoopConf.readFile(file) { (is: InputStream) => fromJSON(JsonMethods.parse(is)) }
    addReference(gr)
    gr
  }

  def compare(contigsIndex: Map[String, Int], c1: String, c2: String): Int = {
    (contigsIndex.get(c1), contigsIndex.get(c2)) match {
      case (Some(i), Some(j)) => i.compare(j)
      case (Some(_), None) => -1
      case (None, Some(_)) => 1
      case (None, None) => c1.compare(c2)
    }
  }

  def compare(contigsIndex: Map[String, Int], v1: IVariant, v2: IVariant): Int = {
    var c = compare(contigsIndex, v1.contig(), v2.contig())
    if (c != 0)
      return c

    c = v1.start().compare(v2.start())
    if (c != 0)
      return c

    c = v1.ref().compare(v2.ref())
    if (c != 0)
      return c

    Ordering.Iterable[AltAllele].compare(v1.altAlleles(), v2.altAlleles())
  }

  def compare(contigsIndex: Map[String, Int], l1: Locus, l2: Locus): Int = {
    val c = compare(contigsIndex, l1.contig, l2.contig)
    if (c != 0)
      return c

    Integer.compare(l1.position, l2.position)
  }

  def compare(contigsIndex: Map[String, Int], i1: Interval[Locus], i2: Interval[Locus]): Int = {
    val c = compare(contigsIndex, i1.start, i2.start)
    if (c != 0)
      return c

    compare(contigsIndex, i1.end, i2.end)
  }

  def gen: Gen[GenomeReference] = for {
    name <- Gen.identifier.filter(!GenomeReference.hasReference(_))
    nContigs <- Gen.choose(3, 10)
    contigs <- Gen.distinctBuildableOfN[Array, String](nContigs, Gen.identifier)
    lengths <- Gen.distinctBuildableOfN[Array, Int](nContigs, Gen.choose(1000000, 500000000))
    contigsIndex = contigs.zip(lengths).toMap
    xContig <- Gen.oneOfSeq(contigs)
    yContig <- Gen.oneOfSeq((contigs.toSet - xContig).toSeq)
    mtContig <- Gen.oneOfSeq((contigs.toSet - xContig - yContig).toSeq)
    locusOrd = new Ordering[Locus] {
      def compare(x: Locus, y: Locus): Int = {
        val c = Integer.compare(contigsIndex(x.contig), contigsIndex(y.contig))
        if (c != 0)
          return c

        Integer.compare(x.position, y.position)
      }
    }
    parX <- Gen.distinctBuildableOfN[Array, Interval[Locus]](2, Interval.gen(Locus.gen(xContig, contigsIndex(xContig)))(locusOrd))
    parY <- Gen.distinctBuildableOfN[Array, Interval[Locus]](2, Interval.gen(Locus.gen(yContig, contigsIndex(yContig)))(locusOrd))
  } yield GenomeReference(name, contigs, contigs.zip(lengths).toMap, Set(xContig), Set(yContig), Set(mtContig),
    (parX ++ parY).map(i => (i.start, i.end)))

  def apply(name: java.lang.String, contigs: java.util.ArrayList[String], lengths: java.util.HashMap[String, Int],
    xContigs: java.util.ArrayList[String], yContigs: java.util.ArrayList[String],
    mtContigs: java.util.ArrayList[String], parInput: java.util.ArrayList[String]): GenomeReference = {
    val parRegex = """(\w+):(\d+)-(\d+)""".r

    val par = parInput.asScala.toArray.map {
        case parRegex(contig, start, end) => (Locus(contig.toString, start.toInt), Locus(contig.toString, end.toInt))
        case _ => fatal("expected PAR input of form contig:start-end")
    }

    val gr = GenomeReference(name, contigs.asScala.toArray, lengths.asScala.toMap, xContigs.asScala.toSet,
      yContigs.asScala.toSet, mtContigs.asScala.toSet, par)
    addReference(gr)
    gr
  }
}

case class GRVariable(var gr: GRBase = null) extends GRBase {

  override def toString = "?GR"

  def unify(concrete: GRBase): Boolean = {
    if (gr == null) {
      gr = concrete
      true
    } else
      gr eq concrete
  }

  def isBound: Boolean = gr != null

  def clear() {
    gr = null
  }

  def subst(): GRBase = {
    assert(gr != null)
    gr
  }

  def name: String = ???

  def variantOrdering: Ordering[Variant] = ???

  def locusOrdering: Ordering[Locus] = ???

  def intervalOrdering: Ordering[Interval[Locus]] = ???

  def isValidContig(contig: String): Boolean = ???

  def isValidPosition(contig: String, start: Int): Boolean = ???

  def checkVariant(v: Variant): Unit = ???

  def checkVariant(contig: String, pos: Int, ref: String, alts: Array[String]): Unit = ???

  def checkVariant(contig: String, pos: Int, ref: String, alt: String): Unit = ???

  def checkVariant(contig: String, start: Int, ref: String, alts: java.util.ArrayList[String]): Unit = ???

  def checkLocus(l: Locus): Unit = ???

  def checkLocus(contig: String, pos: Int): Unit = ???

  def checkInterval(i: Interval[Locus]): Unit = ???

  def checkInterval(l1: Locus, l2: Locus): Unit = ???

  def checkInterval(contig: String, start: Int, end: Int): Unit = ???
  
  def contigLength(contig: String): Int = ???

  def contigLength(contigIdx: Int): Int = ???

  def inX(contigIdx: Int): Boolean = ???

  def inX(contig: String): Boolean = ???

  def inY(contigIdx: Int): Boolean = ???

  def inY(contig: String): Boolean = ???

  def isMitochondrial(contigIdx: Int): Boolean = ???

  def isMitochondrial(contig: String): Boolean = ???

  def inXPar(locus: Locus): Boolean = ???

  def inYPar(locus: Locus): Boolean = ???

  def compare(c1: String, c2: String): Int = ???

  def compare(v1: IVariant, v2: IVariant): Int = ???

  def compare(l1: Locus, l2: Locus): Int = ???

  def toJSON: JValue = ???
}

