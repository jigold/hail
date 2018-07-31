package is.hail.io.bgen

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.ir.{MatrixRead, MatrixReader, MatrixValue}
import is.hail.expr.types._
import is.hail.io._
import is.hail.io.index.{IndexReader, IndexWriter}
import is.hail.io.vcf.LoadVCF
import is.hail.rvd.OrderedRVD
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.sql.Row

import scala.io.Source

case class BgenHeader(
  compressed: Boolean,
  nSamples: Int,
  nVariants: Int,
  headerLength: Int,
  dataStart: Int,
  hasIds: Boolean,
  version: Int,
  fileByteSize: Long,
  path: String
)

object LoadBgen {
  def index(
    mt: MatrixTable,
    files: Array[String],
    rg: Option[String],
    contigRecoding: Map[String, String],
    skipInvalidLoci: Boolean) {

    val conf = new SerializableHadoopConfiguration(mt.hadoopConf)

    val keyFields = mt.rowKey
    val keyType = mt.rowKeyStruct
    val rowType = mt.rvRowType
    val offsetIdx = rowType.fieldIdx("offset")

    val attributes = Map("reference_genome" -> rg.orNull,
      "contig_recoding" -> contigRecoding,
      "skip_invalid_loci" -> skipInvalidLoci)

    mt.rvd.mapPartitionsWithIndex({ (i, it) =>
      val iw = new IndexWriter(conf, files(i), keyType, attributes = attributes)
      val (t, kf) = rowType.select(keyFields.toArray)
      assert(t == keyType)
      it.foreach { rv =>
        val r = UnsafeRow.readBaseStruct(rowType, rv.region, rv.offset)
        iw += (kf(r), r.getLong(offsetIdx))
        rv
      }
      iw.close()
      it
    })
  }

  def readSamples(hConf: org.apache.hadoop.conf.Configuration, file: String): Array[String] = {
    val bState = readState(hConf, file)
    if (bState.hasIds) {
      hConf.readFile(file) { is =>
        val reader = new HadoopFSDataBinaryReader(is)

        reader.seek(bState.headerLength + 4)
        val sampleIdSize = reader.readInt()
        val nSamples = reader.readInt()

        if (nSamples != bState.nSamples)
          fatal("BGEN file is malformed -- number of sample IDs in header does not equal number in file")

        if (sampleIdSize + bState.headerLength > bState.dataStart - 4)
          fatal("BGEN file is malformed -- offset is smaller than length of header")

        (0 until nSamples).map { i =>
          reader.readLengthAndString(2)
        }.toArray
      }
    } else {
      warn(s"BGEN file `$file' contains no sample ID block and no sample ID file given.\n" +
        s"  Using _0, _1, ..., _N as sample IDs.")
      (0 until bState.nSamples).map(i => s"_$i").toArray
    }
  }

  def readSampleFile(hConf: org.apache.hadoop.conf.Configuration, file: String): Array[String] = {
    hConf.readFile(file) { s =>
      Source.fromInputStream(s)
        .getLines()
        .drop(2)
        .filter(line => !line.isEmpty)
        .map { line =>
          val arr = line.split("\\s+")
          arr(0)
        }
        .toArray
    }
  }

  def readState(hConf: org.apache.hadoop.conf.Configuration, file: String): BgenHeader = {
    hConf.readFile(file) { is =>
      val reader = new HadoopFSDataBinaryReader(is)
      readState(reader, file, hConf.getFileSize(file))
    }
  }

  def readState(reader: HadoopFSDataBinaryReader, path: String, byteSize: Long): BgenHeader = {
    reader.seek(0)
    val allInfoLength = reader.readInt()
    val headerLength = reader.readInt()
    val dataStart = allInfoLength + 4

    assert(headerLength <= allInfoLength)
    val nVariants = reader.readInt()
    val nSamples = reader.readInt()

    val magicNumber = reader.readBytes(4)
      .map(_.toInt)
      .toSeq

    if (magicNumber != FastSeq(0, 0, 0, 0) && magicNumber != FastSeq(98, 103, 101, 110))
      fatal(s"expected magic number [0000] or [bgen], got [${ magicNumber.mkString }]")

    if (headerLength > 20)
      reader.skipBytes(headerLength.toInt - 20)

    val flags = reader.readInt()
    val compressType = flags & 3

    if (compressType != 0 && compressType != 1)
      fatal(s"Hail only supports zlib compression.")

    val isCompressed = compressType != 0

    val version = (flags >>> 2) & 0xf
    if (version != 2)
      fatal(s"Hail supports BGEN version 1.2, got version 1.$version")

    val hasIds = (flags >> 31 & 1) != 0
    BgenHeader(
      isCompressed,
      nSamples,
      nVariants,
      headerLength,
      dataStart,
      hasIds,
      version,
      byteSize,
      path
    )
  }
}

case class MatrixBGENReader(
  files: Seq[String],
  sampleFile: Option[String],
  nPartitions: Option[Int],
  blockSizeInMB: Option[Int],
  rg: Option[String],
  contigRecoding: Map[String, String],
  skipInvalidLoci: Boolean,
  includedVariantsPerUnresolvedFilePath: Map[String, Seq[Int]],
  createIndex: Boolean) extends MatrixReader {
  private val hc = HailContext.get
  private val sc = hc.sc
  private val hConf = sc.hadoopConfiguration

  private val referenceGenome = rg.map(ReferenceGenome.getReference)

  private var statuses = hConf.globAllStatuses(files)
  statuses = statuses.flatMap { status =>
    val file = status.getPath.toString
    if (!file.endsWith(".bgen"))
      warn(s"input file does not have .bgen extension: $file")

    if (hConf.isDir(file))
      hConf.listStatus(file)
        .filter(status => ".*part-[0-9]+".r.matches(status.getPath.toString))
    else
      Array(status)
  }

  if (statuses.isEmpty)
    fatal(s"arguments refer to no files: '${ files.mkString(",") }'")

  private val totalSize = statuses.map(_.getLen).sum

  private val inputNPartitions = (blockSizeInMB, nPartitions) match {
    case (Some(blockSizeInMB), _) =>
      val blockSizeInB = blockSizeInMB * 1024 * 1024
      statuses.map { status =>
        val size = status.getLen
        ((size + blockSizeInB - 1) / blockSizeInB).toInt
      }
    case (_, Some(nParts)) =>
      statuses.map { status =>
        val size = status.getLen
        ((size * nParts + totalSize - 1) / totalSize).toInt
      }
    case (None, None) => fatal(s"Must specify either of 'blockSizeInMB' or 'nPartitions'.")
  }

  private val inputs = statuses.map(_.getPath.toString)

  private val sampleIds = sampleFile.map(file => LoadBgen.readSampleFile(hConf, file))
    .getOrElse(LoadBgen.readSamples(hConf, inputs.head))

  LoadVCF.warnDuplicates(sampleIds)

  private val nSamples = sampleIds.length

  val fileHeaders = inputs.map(LoadBgen.readState(hConf, _))

  if (!createIndex) {
    val unequalSamples = fileHeaders.filter(_.nSamples != nSamples).map(x => (x.path, x.nSamples))
    if (unequalSamples.length > 0) {
      val unequalSamplesString =
        unequalSamples.map(x => s"""(${ x._2 } ${ x._1 }""").mkString("\n  ")
      fatal(
        s"""The following BGEN files did not contain the expected number of samples $nSamples:
            |  $unequalSamplesString""".stripMargin)
    }
  }

  val noVariants = fileHeaders.filter(_.nVariants == 0).map(_.path)
  if (noVariants.length > 0)
    fatal(
      s"""The following BGEN files did not contain at least 1 variant:
            |  ${ noVariants.mkString("\n  ") })""".stripMargin)

  val notVersionTwo = fileHeaders.filter(_.version != 2).map(x => x.path -> x.version)
  if (notVersionTwo.length > 0)
    fatal(
      s"""The following BGEN files are not BGENv2:
            |  ${ notVersionTwo.mkString("\n  ") }""".stripMargin)

  val nVariants = fileHeaders.map(_.nVariants).sum

  info(s"Number of BGEN files parsed: ${ fileHeaders.length }")
  info(s"Number of samples in BGEN files: $nSamples")
  info(s"Number of variants across all BGEN files: $nVariants")

  def absolutePath(rel: String): String = {
    val matches = hConf.glob(rel)
    if (matches.length != 1)
      fatal(s"""found more than one match for variant filter path: $rel:
                 |${ matches.mkString(",") }""".stripMargin)
    val abs = matches(0).getPath.toString
    abs
  }

  private val includedVariantsPerFile = toMapIfUnique(
    includedVariantsPerUnresolvedFilePath
  )(absolutePath _
  ) match {
    case Left(duplicatedPaths) =>
      fatal(s"""some relative paths in the import_bgen _variants_per_file
                 |parameter have resolved to the same absolute path
                 |$duplicatedPaths""".stripMargin)
    case Right(m) =>
      log.info(s"variant filters per file after path resolution is $m")
      m
  }

  referenceGenome.foreach(_.validateContigRemap(contigRecoding))

  val outdatedIdxFiles = inputs.filter(f => hConf.exists(f + ".idx") && hConf.isFile(f + ".idx"))
  if (outdatedIdxFiles.length > 0)
    fatal(
      s"""The following BGEN files have invalid index files. Use 'index_bgen' to recreate the index file once before calling 'import_bgen':
            |  ${ outdatedIdxFiles.mkString("\n  ") })""".stripMargin)

  val missingIdxFiles = inputs.filterNot(f => hConf.exists(f + ".idx"))
  if (missingIdxFiles.length > 0)
    fatal(
      s"""The following BGEN files have missing index files. Use 'index_bgen' to create the index file once before calling 'import_bgen':
            |  ${ missingIdxFiles.mkString("\n  ") })""".stripMargin)

  val mismatchIdxFiles = inputs.filter(f => hConf.exists(f + ".idx") && hConf.isDir(f + ".idx")).flatMap { f =>
    val ir = new IndexReader(hConf, f + ".idx")
    val idxAttr = ir.attributes
    val attr = Map("reference_genome" -> rg.orNull, "contig_recoding" -> contigRecoding, "skip_invalid_loci" -> skipInvalidLoci)
    if (idxAttr != attr) {
      val msg = new StringBuilder()
      msg ++= s"The index file for BGEN file '$f' was created with different parameters than called with 'import_bgen':\n"
      Array("reference_genome", "contig_recoding", "skip_invalid_loci").foreach { k =>
        if (idxAttr(k) != attr(k))
          msg ++= s"parameter:$k\texpected:${ idxAttr(k) }\tfound:${ attr(k) }\n"
      }
      Some(msg.result())
    } else
    None
  }
  if (mismatchIdxFiles.length > 0)
    fatal(mismatchIdxFiles.mkString(""))

  val fullType: MatrixType = MatrixType.fromParts(
    globalType = TStruct.empty(),
    colKey = Array("s"),
    colType = TStruct("s" -> TString()),
    rowType = TStruct(
      "locus" -> TLocus.schemaFromRG(referenceGenome),
      "alleles" -> TArray(TString()),
      "rsid" -> TString(),
      "varid" -> TString(),
      "file_row_idx" -> TInt64(),
      "offset" -> TInt64()),
    rowKey = Array("locus", "alleles"),
    rowPartitionKey = Array("locus"),
    entryType = TStruct(
      "GT" -> TCall(),
      "GP" -> +TArray(+TFloat64()),
      "dosage" -> +TFloat64()))

  def columnCount: Option[Int] = Some(nSamples)

  def partitionCounts: Option[IndexedSeq[Long]] = None

  lazy val fastKeys = BgenRDD(
    sc, fileHeaders, inputNPartitions, includedVariantsPerFile,
    BgenSettings(
      nSamples,
      NoEntries,
      RowFields(false, false, false, false),
      referenceGenome,
      contigRecoding,
      skipInvalidLoci,
      createIndex))

  private lazy val coercer = OrderedRVD.makeCoercer(fullType.orvdType, fastKeys, None)

  def apply(mr: MatrixRead): MatrixValue = {
    require(inputs.nonEmpty)

    val requestedType = mr.typ
    val requestedEntryType = requestedType.entryType

    val includeGT = requestedEntryType.hasField("GT")
    val includeGP = requestedEntryType.hasField("GP")
    val includeDosage = requestedEntryType.hasField("dosage")

    val requestedRowType = requestedType.rowType
    val includeLid = requestedRowType.hasField("varid")
    val includeRsid = requestedRowType.hasField("rsid")
    val includeFileRowIdx = requestedRowType.hasField("file_row_idx")
    val includeOffset = requestedRowType.hasField("offset")

    val recordsSettings = BgenSettings(
      nSamples,
      EntriesWithFields(includeGT, includeGP, includeDosage),
      RowFields(includeLid, includeRsid, includeFileRowIdx, includeOffset),
      referenceGenome,
      contigRecoding,
      skipInvalidLoci,
      createIndex)
    assert(mr.typ == recordsSettings.matrixType)

    val rvd = if (mr.dropRows)
      OrderedRVD.empty(sc, requestedType.orvdType)
    else
      coercer.coerce(requestedType.orvdType,
        BgenRDD(sc, fileHeaders, inputNPartitions, includedVariantsPerFile, recordsSettings))

    MatrixValue(mr.typ,
      BroadcastRow(Row.empty, mr.typ.globalType, sc),
      BroadcastIndexedSeq(
        if (mr.dropCols)
          IndexedSeq.empty[Annotation]
        else
          sampleIds.map(x => Annotation(x)),
        TArray(requestedType.colType), sc),
      rvd)
  }
}
