package is.hail.expr.ir

import is.hail.annotations.{Region, UnsafeRow}
import is.hail.annotations.aggregators.{KeyedRegionValueAggregator, _}
import is.hail.asm4s._
import is.hail.expr.types._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.classTag

abstract class BaseCodeAggregator[Agg <: RegionValueAggregator : ClassTag : TypeInfo] {
  def out: Type

  def initOpArgTypes: Option[Array[Class[_]]]

  def seqOpArgTypes: Array[Class[_]]

  def initOp(rva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit]

  def seqOp(region: Code[Region], rva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit]

  def toKeyedAggregator(keyType: Type): KeyedCodeAggregator[Agg]
}

/**
  * Pair the aggregator with a staged seqOp that calls the non-generic seqOp and initOp
  * methods. Missingness is handled by Emit.
  **/
case class CodeAggregator[Agg <: RegionValueAggregator : ClassTag : TypeInfo](
  out: Type,
  constrArgTypes: Array[Class[_]] = Array.empty[Class[_]],
  initOpArgTypes: Option[Array[Class[_]]] = None,
  seqOpArgTypes: Array[Class[_]] = Array.empty[Class[_]]) extends BaseCodeAggregator[Agg] {
  
  def initOp(rva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit] = {
    assert(initOpArgTypes.isDefined && vs.length == ms.length)
    val argTypes = initOpArgTypes.get.flatMap[Class[_], Array[Class[_]]](Array(_, classOf[Boolean]))
    val args = vs.zip(ms).flatMap { case (v, m) => Array(v, m) }
    Code.checkcast[Agg](rva).invoke("initOp", argTypes, args)(classTag[Unit])
  }

  def seqOp(region: Code[Region], rva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit] = {
    assert(vs.length == ms.length)
    val argTypes = seqOpArgTypes.flatMap[Class[_], Array[Class[_]]](Array(_, classOf[Boolean]))
    val args = vs.zip(ms).flatMap { case (v, m) => Array(v, m) }
    Code.checkcast[Agg](rva).invoke("seqOp", Array(classOf[Region]) ++ argTypes, Array(region) ++ args)(classTag[Unit])
  }

  def stagedNew(v: Array[Code[_]], m: Array[Code[Boolean]]): Code[Agg] = {
    assert(v.length == m.length)
    val anyArgMissing = m.fold[Code[Boolean]](false)(_ | _)
    anyArgMissing.mux(
      Code._throw(Code.newInstance[RuntimeException, String]("Aggregators must have non missing constructor arguments")),
      Code.newInstance[Agg](constrArgTypes, v))
  }

  def toKeyedAggregator(keyType: Type): KeyedCodeAggregator[Agg] =
    KeyedCodeAggregator[Agg](Array(keyType), TDict(keyType, out), initOpArgTypes, seqOpArgTypes)
}

case class KeyedCodeAggregator[Agg <: RegionValueAggregator : ClassTag : TypeInfo](
  keys: Array[Type],
  out: Type,
  initOpArgTypes: Option[Array[Class[_]]] = None,
  seqOpArgTypes: Array[Class[_]] = Array.empty[Class[_]]) extends BaseCodeAggregator[Agg] {

  def initOp(krva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit] = {
    assert(initOpArgTypes.isDefined && vs.length == ms.length)
    val argTypes = initOpArgTypes.get.flatMap[Class[_], Array[Class[_]]](Array(_, classOf[Boolean]))
    val args = vs.zip(ms).flatMap { case (v, m) => Array(v, m) }
    val krvAgg = Code.checkcast[KeyedRegionValueAggregator[Agg]](krva)
    krvAgg.invoke[Agg]("rvAgg").invoke("initOp", argTypes, args)(classTag[Unit])
  }

  def getRVAgg(region: Code[Region], krva: Code[KeyedRegionValueAggregator[_]], keyTypes: Array[Type], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[RegionValueAggregator] = {
    def wrapArg(arg: Code[_]): Code[_] = keyTypes.head match {
      case _: TBoolean => Code.boxBoolean(coerce[Boolean](arg))
      case _: TInt32 | _: TCall => Code.boxInt(coerce[Int](arg))
      case _: TInt64 => Code.boxLong(coerce[Long](arg))
      case _: TFloat32 => Code.boxFloat(coerce[Float](arg))
      case _: TFloat64 => Code.boxDouble(coerce[Double](arg))
      case _: TString =>
        Code.invokeScalaObject[Region, Long, String](
          TString.getClass, "loadString",
          region, coerce[Long](arg))
      case _ =>
        Code.invokeScalaObject[Type, Region, Long, Any](
          UnsafeRow.getClass, "read",
          krva.invoke[Type]("keyType"), region, coerce[Long](arg))
    }

    val wrappedKey = ms.head.mux(Code._null[Any], wrapArg(vs.head))
    val m = krva.invoke[mutable.Map[Any, Agg]]("m")

    if (keyTypes.length == 1) {
      Code(m.invoke[Any, Boolean]("contains", wrappedKey).mux(
        Code._empty,
        m.invoke[Any, Any, Unit]("update", wrappedKey, krva.invoke[RegionValueAggregator]("rvAgg").invoke[Agg]("copy"))),
        m.invoke("apply", wrappedKey))
    } else {
      val newkrvAgg = Code.checkcast[KeyedRegionValueAggregator[_]](Code(m.invoke[Any, Boolean]("contains", wrappedKey).mux(
        Code._empty,
        m.invoke[Any, Any, Unit]("update", wrappedKey, krva.invoke[RegionValueAggregator]("rvAgg").invoke[KeyedRegionValueAggregator[Agg]]("copy"))),
        m.invoke("apply", wrappedKey)))
      getRVAgg(region, newkrvAgg, keyTypes.drop(1), vs.drop(1), ms.drop(1))
    }
  }

  def seqOp(region: Code[Region], krva: Code[RegionValueAggregator], vs: Array[Code[_]], ms: Array[Code[Boolean]]): Code[Unit] = {
    assert(vs.length == ms.length)

    val krvAgg = Code.checkcast[KeyedRegionValueAggregator[_]](krva)
    val rva = getRVAgg(region, krvAgg, keys, vs, ms)

    val nKeys = keys.length

    val argTypes = classOf[Region] +: seqOpArgTypes.flatMap[Class[_], Array[Class[_]]](Array(_, classOf[Boolean]))
    val args = vs.drop(nKeys).zip(ms.drop(nKeys)).flatMap { case (v, m) => Array(v, m) }

    Code.checkcast[Agg](rva).invoke("seqOp", argTypes, Array(region) ++ args)(classTag[Unit])
  }

  def toKeyedAggregator(keyType: Type): KeyedCodeAggregator[Agg] =
    KeyedCodeAggregator[Agg](keys ++ Array(keyType), TDict(keyType, out), initOpArgTypes, seqOpArgTypes)
}
