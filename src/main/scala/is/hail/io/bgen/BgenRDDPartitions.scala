package is.hail.io.bgen

import is.hail.annotations.{Region, _}
import is.hail.asm4s._
import is.hail.expr.ir.TableValue
import is.hail.expr.types._
import is.hail.io.{ByteArrayReader, HadoopFSDataBinaryReader}
import is.hail.io.index.{IndexReader, LeafChild}
import is.hail.rvd.OrderedRVDPartitioner
import is.hail.utils._
import is.hail.variant.{Call2, ReferenceGenome}
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, SparkContext}

trait BgenPartition extends Partition {
  def path: String

  def compressed: Boolean

  def makeInputStream: HadoopFSDataBinaryReader

  // advances the reader to the next variant position
  def advance(bfis: HadoopFSDataBinaryReader): Unit

  def hasNext(bfis: HadoopFSDataBinaryReader): Boolean
}

object BgenRDDPartitions extends Logging {
  def apply(
    sc: SparkContext,
    files: Seq[BgenHeader],
    fileNPartitions: Array[Int],
    includedVariants: TableValue,
    createIndex: Boolean
  ): Array[Partition] = {
    val hConf = sc.hadoopConfiguration
    val sHadoopConfBc = sc.broadcast(new SerializableHadoopConfiguration(hConf))

    val updatedFiles = files.map { header =>
      val nVariants =
        if (createIndex)
          header.nVariants
        else {
          // This is needed because invalid loci can be skipped when creating the index
          val nIndexKeys = IndexReader.readMetadata(hConf, header.path + ".idx/metadata.json.gz").nKeys.toInt
          assert(nIndexKeys <= header.nVariants)
          nIndexKeys
        }
      header.copy(nVariants = nVariants)
    }

    val nonEmptyFiles = updatedFiles.filter(_.nVariants > 0)

    if (nonEmptyFiles.isEmpty) {
      Array.empty
    } else {
      val partitions = new ArrayBuilder[Partition]()
      var fileIndex = 0
      while (fileIndex < nonEmptyFiles.length) {
        val file = nonEmptyFiles(fileIndex)

        val nPartitions = fileNPartitions(fileIndex)
        if (createIndex) {
          assert(nPartitions == 1)
          partitions += IndexBgenPartition(
            file.path,
            file.compressed,
            0,
            file.dataStart,
            file.fileByteSize,
            partitions.length,
            sHadoopConfBc
          )
        } else {
          using(new IndexReader(hConf, file.path + ".idx")) { index =>
            val partNVariants = partition(file.nVariants, nPartitions)
            val partFirstVariantIndex = partNVariants.scan(0)(_ + _).init

            if (includedVariants != null) {
              val keyType = includedVariants.typ.keyType
              val partFirstKeys = partFirstVariantIndex.map(index.queryByIndex(_).key)

              val intervals = {
                val ab = new ArrayBuilder[Interval]()
                val n = partFirstKeys.length
                var i = 0
                while (i < n) {
                  val interval =
                    if (i != n - 1)
                      Interval(partFirstKeys(i), partFirstKeys(i + 1), includesStart = true, includesEnd = true)
                    else
                      Interval(partFirstKeys(i), index.queryByIndex(index.nKeys - 1).key, includesStart = true, includesEnd = true)
                  ab += interval
                  i += 1
                }
                ab.result()
              }

              val newPartitioner = OrderedRVDPartitioner.generate(keyType.get, intervals)
              val keysRDD = includedVariants.enforceOrderingRVD.toOrderedRVD.constrainToOrderedPartitioner(newPartitioner).toRows
              val keyIterators = sc.runJob(keysRDD, (it: Iterator[Annotation]) => it)

              var i = 0
              while (i < nPartitions) {
                partitions += BgenPartitionFromKeys(
                  file.path,
                  file.compressed,
                  partitions.length,
                  index,
                  keyIterators(i),
                  sHadoopConfBc
                )
                i += 1
              }
              assert(i == keyIterators.length)
            } else {
              var i = 0
              while (i < nPartitions) {
                val firstVariantIndex = partFirstVariantIndex(i)
                val lastVariantIndex = firstVariantIndex + partNVariants(i)

                partitions += BgenPartitionFromRange(
                  file.path,
                  file.compressed,
                  partitions.length,
                  index.iterator(firstVariantIndex, lastVariantIndex),
                  //                  index,
                  //                  firstVariantIndex,
                  //                  lastVariantIndex,
                  sHadoopConfBc
                )
                i += 1
              }
            }
          }
        }
        fileIndex += 1
      }
      partitions.result()
    }
  }

  private case class IndexBgenPartition(
    path: String,
    compressed: Boolean,
    firstRecordIndex: Long,
    startByteOffset: Long,
    endByteOffset: Long,
    partitionIndex: Int,
    sHadoopConfBc: Broadcast[SerializableHadoopConfiguration]
  ) extends BgenPartition {

    def index = partitionIndex

    def makeInputStream = {
      val hadoopPath = new Path(path)
      val fs = hadoopPath.getFileSystem(sHadoopConfBc.value.value)
      val bfis = new HadoopFSDataBinaryReader(fs.open(hadoopPath))
      bfis.seek(startByteOffset)
      bfis
    }

    def advance(bfis: HadoopFSDataBinaryReader) {
    }

    def hasNext(bfis: HadoopFSDataBinaryReader): Boolean =
      bfis.getPosition < endByteOffset
  }

  private case class BgenPartitionFromRange(
    path: String,
    compressed: Boolean,
    partitionIndex: Int,
    it: Iterator[LeafChild],
    sHadoopConfBc: Broadcast[SerializableHadoopConfiguration]
  ) extends BgenPartition {
    def index = partitionIndex

    def makeInputStream = {
      val hadoopPath = new Path(path)
      val fs = hadoopPath.getFileSystem(sHadoopConfBc.value.value)
      new HadoopFSDataBinaryReader(fs.open(hadoopPath))
    }

    def advance(bfis: HadoopFSDataBinaryReader) {
      val newPos = it.next().recordOffset
      if (newPos != bfis.getPosition)
        bfis.seek(newPos)
    }

    def hasNext(bfis: HadoopFSDataBinaryReader): Boolean = it.hasNext
  }

  private case class BgenPartitionFromKeys(
    path: String,
    compressed: Boolean,
    partitionIndex: Int,
    indexReader: IndexReader,
    keys: Iterator[Annotation],
    sHadoopConfBc: Broadcast[SerializableHadoopConfiguration]
  ) extends BgenPartition {
    private[this] var current: Annotation = _
    private[this] var prev: Annotation = _
    private[this] var indexKeyIterator: Iterator[LeafChild] = _
    private[this] var isEnd = false
    updateIterators()

    def updateIterators() {
      while (indexKeyIterator == null || (keys.hasNext && !indexKeyIterator.hasNext)) {
        do {
          prev = current
          current = keys.next()
          indexKeyIterator = indexReader.keyIterator(current)
        } while (keys.hasNext && current == prev)
      }

      isEnd = !keys.hasNext && !indexKeyIterator.hasNext
    }

    def index = partitionIndex

    def makeInputStream = {
      val hadoopPath = new Path(path)
      val fs = hadoopPath.getFileSystem(sHadoopConfBc.value.value)
      new HadoopFSDataBinaryReader(fs.open(hadoopPath))
    }

    def advance(bfis: HadoopFSDataBinaryReader) {
      assert(!isEnd)

      val newPos = indexKeyIterator.next().recordOffset
      if (newPos != bfis.getPosition)
        bfis.seek(newPos)

      updateIterators()
    }

    def hasNext(bfis: HadoopFSDataBinaryReader): Boolean = isEnd
  }
}

object CompileDecoder {
  def apply(
    settings: BgenSettings
  ): () => AsmFunction4[Region, BgenPartition, HadoopFSDataBinaryReader, BgenSettings, Long] = {
    val fb = new Function4Builder[Region, BgenPartition, HadoopFSDataBinaryReader, BgenSettings, Long]
    val mb = fb.apply_method
    val cp = mb.getArg[BgenPartition](2).load()
    val cbfis = mb.getArg[HadoopFSDataBinaryReader](3).load()
    val csettings = mb.getArg[BgenSettings](4).load()
    val srvb = new StagedRegionValueBuilder(mb, settings.typ)
    val offset = mb.newLocal[Long]
    val varid = mb.newLocal[String]
    val rsid = mb.newLocal[String]
    val contig = mb.newLocal[String]
    val contigRecoded = mb.newLocal[String]
    val position = mb.newLocal[Int]
    val nAlleles = mb.newLocal[Int]
    val i = mb.newLocal[Int]
    val dataSize = mb.newLocal[Int]
    val invalidLocus = mb.newLocal[Boolean]
    val data = mb.newLocal[Array[Byte]]
    val uncompressedSize = mb.newLocal[Int]
    val input = mb.newLocal[Array[Byte]]
    val reader = mb.newLocal[ByteArrayReader]
    val nRow = mb.newLocal[Int]
    val nAlleles2 = mb.newLocal[Int]
    val minPloidy = mb.newLocal[Int]
    val maxPloidy = mb.newLocal[Int]
    val longPloidy = mb.newLocal[Long]
    val ploidy = mb.newLocal[Int]
    val phase = mb.newLocal[Int]
    val nBitsPerProb = mb.newLocal[Int]
    val nExpectedBytesProbs = mb.newLocal[Int]
    val c0 = mb.newLocal[Int]
    val c1 = mb.newLocal[Int]
    val c2 = mb.newLocal[Int]
    val off = mb.newLocal[Int]
    val d0 = mb.newLocal[Int]
    val d1 = mb.newLocal[Int]
    val d2 = mb.newLocal[Int]
    val c = Code(
      cp.invoke[HadoopFSDataBinaryReader, Unit]("advance", cbfis),
      offset := cbfis.invoke[Long]("getPosition"),
      if (settings.rowFields.varid) {
        varid := cbfis.invoke[Int, String]("readLengthAndString", 2)
      } else {
        cbfis.invoke[Int, Unit]("readLengthAndSkipString", 2)
      },
      if (settings.rowFields.rsid) {
        rsid := cbfis.invoke[Int, String]("readLengthAndString", 2)
      } else {
        cbfis.invoke[Int, Unit]("readLengthAndSkipString", 2)
      },
      contig := cbfis.invoke[Int, String]("readLengthAndString", 2),
      contigRecoded := csettings.invoke[String, String]("recodeContig", contig),
      position := cbfis.invoke[Int]("readInt"),
      if (settings.skipInvalidLoci) {
        Code(
          invalidLocus :=
            (if (settings.rg.nonEmpty) {
              !csettings.invoke[Option[ReferenceGenome]]("rg")
                .invoke[ReferenceGenome]("get")
                .invoke[String, Int, Boolean]("isValidLocus", contigRecoded, position)
            } else false),
          invalidLocus.mux(
            Code(
              nAlleles := cbfis.invoke[Int]("readShort"),
              i := 0,
              Code.whileLoop(i < nAlleles,
                cbfis.invoke[Int, Unit]("readLengthAndSkipString", 4),
                i := i + 1
              ),
              dataSize := cbfis.invoke[Int]("readInt"),
              Code.toUnit(cbfis.invoke[Long, Long]("skipBytes", dataSize.toL)),
              Code._return(-1L) // return -1 to indicate we are skipping this variant
            ),
            Code._empty // if locus is valid, continue
          ))
      } else {
        if (settings.rg.nonEmpty) {
          // verify the locus is valid before continuing
          csettings.invoke[Option[ReferenceGenome]]("rg")
            .invoke[ReferenceGenome]("get")
            .invoke[String, Int, Unit]("checkLocus", contigRecoded, position)
        } else {
          Code._empty // if locus is valid continue
        }
      },
      srvb.start(),
      srvb.addBaseStruct(settings.typ.types(settings.typ.fieldIdx("locus")).fundamentalType.asInstanceOf[TBaseStruct], { srvb =>
        Code(
          srvb.start(),
          srvb.addString(contigRecoded),
          srvb.advance(),
          srvb.addInt(position))
      }),
      srvb.advance(),
      nAlleles := cbfis.invoke[Int]("readShort"),
      nAlleles.cne(2).mux(
        Code._fatal(
          const("Only biallelic variants supported, found variant with ")
            .concat(nAlleles.toS)),
        Code._empty),
      srvb.addArray(settings.typ.types(settings.typ.fieldIdx("alleles")).asInstanceOf[TArray], { srvb =>
        Code(
          srvb.start(nAlleles),
          i := 0,
          Code.whileLoop(i < nAlleles,
            srvb.addString(cbfis.invoke[Int, String]("readLengthAndString", 4)),
            srvb.advance(),
            i := i + 1))
      }),
      srvb.advance(),
      if (settings.rowFields.rsid)
        Code(srvb.addString(rsid), srvb.advance())
      else Code._empty,
      if (settings.rowFields.varid)
        Code(srvb.addString(varid), srvb.advance())
      else Code._empty,
      if (settings.rowFields.offset)
        Code(srvb.addLong(offset), srvb.advance())
      else Code._empty,
      dataSize := cbfis.invoke[Int]("readInt"),
      settings.entries match {
        case NoEntries =>
          Code.toUnit(cbfis.invoke[Long, Long]("skipBytes", dataSize.toL))

        case EntriesWithFields(gt, gp, dosage) if !(gt || gp || dosage) =>
          assert(settings.matrixType.entryType.byteSize == 0)
          Code(
            srvb.addArray(settings.matrixType.entryArrayType, { srvb =>
              Code(
                srvb.start(settings.nSamples),
                i := 0,
                Code.whileLoop(i < settings.nSamples,
                  srvb.advance(),
                  i := i + 1))
            }),
            srvb.advance(),
            Code.toUnit(cbfis.invoke[Long, Long]("skipBytes", dataSize.toL)))

        case EntriesWithFields(includeGT, includeGP, includeDosage) =>
          Code(
            cp.invoke[Boolean]("compressed").mux(
              Code(
                uncompressedSize := cbfis.invoke[Int]("readInt"),
                input := cbfis.invoke[Int, Array[Byte]]("readBytes", dataSize - 4),
                data := Code.invokeScalaObject[Array[Byte], Int, Array[Byte]](
                  BgenRDD.getClass, "decompress", input, uncompressedSize)),
              data := cbfis.invoke[Int, Array[Byte]]("readBytes", dataSize)),
            reader := Code.newInstance[ByteArrayReader, Array[Byte]](data),
            nRow := reader.invoke[Int]("readInt"),
            (nRow.cne(settings.nSamples)).mux(
              Code._fatal(
                const("Row nSamples is not equal to header nSamples: ")
                  .concat(nRow.toS)
                  .concat(", ")
                  .concat(settings.nSamples.toString)),
              Code._empty),
            nAlleles2 := reader.invoke[Int]("readShort"),
            (nAlleles.cne(nAlleles2)).mux(
              Code._fatal(
                const("""Value for `nAlleles' in genotype probability data storage is
                        |not equal to value in variant identifying data. Expected""".stripMargin)
                  .concat(nAlleles.toS)
                  .concat(" but found ")
                  .concat(nAlleles2.toS)
                  .concat(" at ")
                  .concat(contig)
                  .concat(":")
                  .concat(position.toS)
                  .concat(".")),
              Code._empty),
            minPloidy := reader.invoke[Int]("read"),
            maxPloidy := reader.invoke[Int]("read"),
            (minPloidy.cne(2) || maxPloidy.cne(2)).mux(
              Code._fatal(
                const("Hail only supports diploid genotypes. Found min ploidy `")
                  .concat(minPloidy.toS)
                  .concat("' and max ploidy `")
                  .concat(maxPloidy.toS)
                  .concat("'.")),
              Code._empty),
            i := 0,
            Code.whileLoop(i < settings.nSamples,
              ploidy := reader.invoke[Int]("read"),
              ((ploidy & 0x3f).cne(2)).mux(
                Code._fatal(
                  const("Ploidy value must equal to 2. Found ")
                    .concat(ploidy.toS)
                    .concat(".")),
                Code._empty),
              i += 1
            ),
            phase := reader.invoke[Int]("read"),
            (phase.cne(0) && phase.cne(1)).mux(
              Code._fatal(
                const("Phase value must be 0 or 1. Found ")
                  .concat(phase.toS)
                  .concat(".")),
              Code._empty),
            phase.ceq(1).mux(
              Code._fatal("Hail does not support phased genotypes."),
              Code._empty),
            nBitsPerProb := reader.invoke[Int]("read"),
            (nBitsPerProb < 1 || nBitsPerProb > 32).mux(
              Code._fatal(
                const("nBits value must be between 1 and 32 inclusive. Found ")
                  .concat(nBitsPerProb.toS)
                  .concat(".")),
              Code._empty),
            (nBitsPerProb.cne(8)).mux(
              Code._fatal(
                const("Hail only supports 8-bit probabilities, found ")
                  .concat(nBitsPerProb.toS)
                  .concat(".")),
              Code._empty),
            nExpectedBytesProbs := settings.nSamples * 2,
            (reader.invoke[Int]("length").cne(nExpectedBytesProbs + settings.nSamples + 10)).mux(
              Code._fatal(
                const("Number of uncompressed bytes `")
                  .concat(reader.invoke[Int]("length").toS)
                  .concat("' does not match the expected size `")
                  .concat(nExpectedBytesProbs.toS)
                  .concat("'.")),
              Code._empty),
            c0 := Call2.fromUnphasedDiploidGtIndex(0),
            c1 := Call2.fromUnphasedDiploidGtIndex(1),
            c2 := Call2.fromUnphasedDiploidGtIndex(2),
            srvb.addArray(settings.matrixType.entryArrayType, { srvb =>
              Code(
                srvb.start(settings.nSamples),
                i := 0,
                Code.whileLoop(i < settings.nSamples,
                  (data(i + 8) & 0x80).cne(0).mux(
                    srvb.setMissing(),
                    srvb.addBaseStruct(settings.matrixType.entryType, { srvb =>
                      Code(
                        srvb.start(),
                        off := const(settings.nSamples + 10) + i * 2,
                        d0 := data(off) & 0xff,
                        d1 := data(off + 1) & 0xff,
                        d2 := const(255) - d0 - d1,
                        if (includeGT) {
                          Code(
                            (d0 > d1).mux(
                              (d0 > d2).mux(
                                srvb.addInt(c0),
                                (d2 > d0).mux(
                                  srvb.addInt(c2),
                                  // d0 == d2
                                  srvb.setMissing())),
                              // d0 <= d1
                              (d2 > d1).mux(
                                srvb.addInt(c2),
                                // d2 <= d1
                                (d1.ceq(d0) || d1.ceq(d2)).mux(
                                  srvb.setMissing(),
                                  srvb.addInt(c1)))),
                            srvb.advance())
                        } else Code._empty,
                        if (includeGP) {
                          Code(
                            srvb.addArray(settings.matrixType.entryType.types(settings.matrixType.entryType.fieldIdx("GP")).asInstanceOf[TArray], { srvb =>
                              Code(
                                srvb.start(3),
                                srvb.addDouble(d0.toD / 255.0),
                                srvb.advance(),
                                srvb.addDouble(d1.toD / 255.0),
                                srvb.advance(),
                                srvb.addDouble(d2.toD / 255.0),
                                srvb.advance())
                            }),
                            srvb.advance())
                        } else Code._empty,
                        if (includeDosage) {
                          val dosage = (d1 + (d2 << 1)).toD / 255.0
                          Code(
                            srvb.addDouble(dosage),
                            srvb.advance())
                        } else Code._empty)
                    })),
                  srvb.advance(),
                  i := i + 1))
            }))
      },
      srvb.end())

    mb.emit(c)
    fb.result()
  }
}
