package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.keytable.KeyTable
import org.broadinstitute.hail.utils._
import org.testng.annotations.Test

class KeyTableSuite extends SparkSuite {

  @Test def testSingleToPairRDD() = {
    val inputFile = "src/test/resources/sampleAnnotations.tsv"
    val kt = KeyTable.importTextTable(sc, Array(inputFile), "Sample, Status", sc.defaultMinPartitions, TextTableConfiguration())
    val kt2 = KeyTable(KeyTable.toSingleRDD(kt.rdd, kt.nKeys, kt.nValues), kt.signature, kt.keyNames)

    assert(kt.rdd.fullOuterJoin(kt2.rdd).forall { case (k, (v1, v2)) =>
      val res = v1 == v2
      if (!res)
        println(s"k=$k v1=$v1 v2=$v2 res=${ v1 == v2 }")
      res
    })
  }

  @Test def testImportExport() = {
    val inputFile = "src/test/resources/sampleAnnotations.tsv"
    val outputFile = tmpDir.createTempFile("ktImpExp", "tsv")
    val kt = KeyTable.importTextTable(sc, Array(inputFile), "Sample, Status", sc.defaultMinPartitions, TextTableConfiguration())
    kt.export(sc, outputFile, null)

    val importedData = sc.hadoopConfiguration.readLines(inputFile)(_.map(_.value).toIndexedSeq)
    val exportedData = sc.hadoopConfiguration.readLines(outputFile)(_.map(_.value).toIndexedSeq)

    intercept[FatalException] {
      val kt2 = KeyTable.importTextTable(sc, Array(inputFile), "Sample, Status, BadKeyName", sc.defaultMinPartitions, TextTableConfiguration())
    }

    assert(importedData == exportedData)
  }

  @Test def testAnnotate() = {
    val inputFile = "src/test/resources/sampleAnnotations.tsv"
    val kt1 = KeyTable.importTextTable(sc, Array(inputFile), "Sample", sc.defaultMinPartitions, TextTableConfiguration(impute = true))
    val kt2 = kt1.annotate("""qPhen2 = pow(qPhen, 2), NotStatus = Status == "CASE", X = qPhen == 5""", kt1.keyNames.mkString(","))
    val kt3 = kt2.annotate("", kt2.keyNames.mkString(","))
    val kt4 = kt3.annotate("", "qPhen, NotStatus")

    val kt1ValueNames = kt1.valueNames.toSet
    val kt2ValueNames = kt2.valueNames.toSet

    assert(kt1.nKeys == 1)
    assert(kt2.nKeys == 1)
    assert(kt1.nValues == 2 && kt2.nValues == 5)
    assert(kt1.keySignature == kt2.keySignature)
    assert(kt1ValueNames ++ Set("qPhen2", "NotStatus", "X") == kt2ValueNames)
    assert(kt2 same kt3)

    def getDataAsMap(kt: KeyTable) = {
      val fieldNames = kt.fieldNames
      val nFields = kt.nFields
      KeyTable.toSingleRDD(kt.rdd, kt.nKeys, kt.nValues)
        .map { a => fieldNames.zip(KeyTable.annotationToSeq(a, nFields)).toMap }.collect().toSet
    }

    val kt3data = getDataAsMap(kt3)
    val kt4data = getDataAsMap(kt4)

    assert(kt4.keyNames.toSet == Set("qPhen", "NotStatus") &&
      kt4.valueNames.toSet == Set("qPhen2", "X", "Sample", "Status") &&
      kt3data == kt4data
    )
  }

  @Test def testFilter() = {
    val data = Array(Array(5, 9, 0), Array(2, 3, 4), Array(1, 2, 3))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("field1", TInt), ("field2", TInt), ("field3", TInt))
    val keyNames = Array("field1")

    val kt1 = KeyTable(rdd, signature, keyNames)
    val kt2 = kt1.filter("field1 < 3", keep = true)
    val kt3 = kt1.filter("field1 < 3 && field3 == 4", keep = true)
    val kt4 = kt1.filter("field1 == 5 && field2 == 9 && field3 == 0", keep = false)
    val kt5 = kt1.filter("field1 < -5 && field3 == 100", keep = true)

    assert(kt1.nRows == 3 && kt2.nRows == 2 && kt3.nRows == 1 && kt4.nRows == 2 && kt5.nRows == 0)
  }

  @Test def testJoin() = {
    val inputFile1 = "src/test/resources/sampleAnnotations.tsv"
    val inputFile2 = "src/test/resources/sampleAnnotations2.tsv"

    val ktLeft = KeyTable.importTextTable(sc, Array(inputFile1), "Sample", sc.defaultMinPartitions, TextTableConfiguration(impute = true))
    val ktRight = KeyTable.importTextTable(sc, Array(inputFile2), "Sample", sc.defaultMinPartitions, TextTableConfiguration(impute = true))

    val ktLeftJoin = ktLeft.leftJoin(ktRight)
    val ktRightJoin = ktLeft.rightJoin(ktRight)
    val ktInnerJoin = ktLeft.innerJoin(ktRight)
    val ktOuterJoin = ktLeft.outerJoin(ktRight)

    val nExpectedValues = ktLeft.nValues + ktRight.nValues

    val (_, leftKeyQuery) = ktLeft.query("Sample")
    val (_, rightKeyQuery) = ktRight.query("Sample")
    val (_, leftJoinKeyQuery) = ktLeftJoin.query("Sample")
    val (_, rightJoinKeyQuery) = ktRightJoin.query("Sample")

    val leftKeys = ktLeft.rdd.map { case (k, v) => leftKeyQuery(k, v).map(_.asInstanceOf[String]) }.collect().toSet
    val rightKeys = ktRight.rdd.map { case (k, v) => rightKeyQuery(k, v).map(_.asInstanceOf[String]) }.collect().toSet

    val nIntersectRows = leftKeys.intersect(rightKeys).size
    val nUnionRows = rightKeys.union(leftKeys).size
    val nExpectedKeys = ktLeft.nKeys

    assert(ktLeftJoin.nRows == ktLeft.nRows &&
      ktLeftJoin.nKeys == nExpectedKeys &&
      ktLeftJoin.nValues == nExpectedValues &&
      ktLeftJoin.filter { case (k, v) =>
        !rightKeys.contains(leftJoinKeyQuery(k, v).map(_.asInstanceOf[String]))
      }.forall("isMissing(qPhen2) && isMissing(qPhen3)")
    )

    assert(ktRightJoin.nRows == ktRight.nRows &&
      ktRightJoin.nKeys == nExpectedKeys &&
      ktRightJoin.nValues == nExpectedValues &&
      ktRightJoin.filter { case (k, v) =>
        !leftKeys.contains(rightJoinKeyQuery(k, v).map(_.asInstanceOf[String]))
      }.forall("isMissing(Status) && isMissing(qPhen)"))

    assert(ktOuterJoin.nRows == nUnionRows &&
      ktOuterJoin.nKeys == ktLeft.nKeys &&
      ktOuterJoin.nValues == nExpectedValues)

    assert(ktInnerJoin.nRows == nIntersectRows &&
      ktInnerJoin.nKeys == nExpectedKeys &&
      ktInnerJoin.nValues == nExpectedValues)
  }

  @Test def testAggregate() {
    val data = Array(Array("Case", 9, 0), Array("Case", 3, 4), Array("Control", 2, 3), Array("Control", 1, 5))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("field1", TString), ("field2", TInt), ("field3", TInt))
    val keyNames = Array("field1")

    val kt1 = KeyTable(rdd, signature, keyNames)
    val kt2 = kt1.aggregate("Status = field1",
      "A = field2.sum(), " +
      "B = field2.map(f => field2).sum(), " +
      "C = field2.map(f => field2 + field3).sum(), " +
      "D = field2.count(), " +
      "E = field2.filter(f => field2 == 3).count()"
    )

    val result = Array(Array("Case", 12, 12, 16, 2L, 1L), Array("Control", 3, 3, 11, 2L, 0L))
    val resRDD = sc.parallelize(result.map(Annotation.fromSeq(_)))
    val resSignature = TStruct(("Status", TString), ("A", TInt), ("B", TInt), ("C", TInt), ("D", TLong), ("E", TLong))
    val ktResult = KeyTable(resRDD, resSignature, keyNames = Array("Status"))

    assert(kt2 same ktResult)
  }

  @Test def testForallExists() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("Sample", TString), ("field1", TInt), ("field2", TInt))
    val keyNames = Array("Sample")

    val kt = KeyTable(rdd, signature, keyNames)
    assert(kt.forall("field2 == 5 && field1 != 0"))
    assert(!kt.forall("field2 == 0 && field1 == 5"))
    assert(kt.exists("""Sample == "Sample1" && field1 == 9 && field2 == 5"""))
    assert(!kt.exists("""Sample == "Sample1" && field1 == 13 && field2 == 2"""))
  }

  @Test def testRename() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("Sample", TString), ("field1", TInt), ("field2", TInt))
    val keyNames = Array("Sample")

    val kt = KeyTable(rdd, signature, keyNames)

    val rename1 = kt.rename(Array("ID1", "ID2", "ID3"))
    assert(rename1.fieldNames sameElements Array("ID1", "ID2", "ID3"))

    val rename2 = kt.rename(Map("field1" -> "ID1"))
    assert(rename2.fieldNames sameElements Array("Sample", "ID1", "field2"))

    intercept[FatalException](kt.rename(Array("ID1")))

    intercept[FatalException](kt.rename(Map("field1" -> "field2")))

    intercept[FatalException](kt.rename(Map("Sample" -> "field2", "field1" -> "field2")))
  }

  @Test def testSelect() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("Sample", TString), ("field1", TInt), ("field2", TInt))
    val keyNames = Array("Sample")

    val kt = KeyTable(rdd, signature, keyNames)

    val select1 = kt.select(Array("field1"), Array("field1"))
    assert((select1.keyNames sameElements Array("field1")) && (select1.valueNames sameElements Array.empty[String]))

    val select2 = kt.select(Array("Sample", "field2", "field1"), Array("Sample"))
    assert((select2.keyNames sameElements Array("Sample")) && (select2.valueNames sameElements Array("field2", "field1")))

    val select3 = kt.select(Array("field2", "field1", "Sample"), Array.empty[String])
    assert((select3.keyNames sameElements Array.empty[String]) && (select3.valueNames sameElements Array("field2", "field1", "Sample")))

    val select4 = kt.select(Array.empty[String], Array.empty[String])
    assert((select4.keyNames sameElements Array.empty[String]) && (select4.valueNames sameElements Array.empty[String]))

    intercept[FatalException](kt.select(Array.empty[String], Array("Sample")))

    intercept[FatalException](kt.select(Array("Sample", "field2", "field5"), Array("Sample")))
  }
}
