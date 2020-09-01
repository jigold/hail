package is.hail.types.encoded

import is.hail.annotations.{Region, UnsafeUtils}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.types.BaseType
import is.hail.types.physical._
import is.hail.types.virtual._
import is.hail.io.{InputBuffer, OutputBuffer}
import is.hail.utils._

final case class EArray(val elementType: EType, override val required: Boolean = false) extends EContainer with EFundamentalType {
  override def _decodeCompatible(pt: PType): Boolean = {
    pt.required <= required &&
      pt.isInstanceOf[PArray] &&
      elementType.decodeCompatible(pt.asInstanceOf[PArray].elementType)
  }

  override def _encodeCompatible(pt: PType): Boolean = {
    pt.required >= required &&
      pt.isInstanceOf[PArray] &&
      elementType.encodeCompatible(pt.asInstanceOf[PArray].elementType)
  }

  def _decodedPType(requestedType: Type): PType = {
    val elementPType = elementType.decodedPType(requestedType.asInstanceOf[TContainer].elementType)
    requestedType match {
      case _: TSet =>
        PCanonicalSet(elementPType, required)
      case _: TArray =>
        PCanonicalArray(elementPType, required)
      case _: TDict =>
        val et = elementPType.asInstanceOf[PStruct]
        PCanonicalDict(et.fieldType("key"), et.fieldType("value"), required)
    }
  }

  def buildPrefixEncoder(cb: EmitCodeBuilder, pt: PArray, array: Value[Long],
    out: Value[OutputBuffer], prefixLength: Code[Int]
  ): Unit = {
    val arr: PIndexableValue = PCode(pt, array).asIndexable.memoize(cb, "encode_array_a")
    val prefixLen = cb.newLocal[Int]("prefixLen", prefixLength)
    val i = cb.newLocal[Int]("i", 0)

    cb += out.writeInt(prefixLen)

    pt match {
      case t: PCanonicalArray if t.elementType.required == elementType.required =>
        if (!elementType.required) {
          val nMissingLocal = cb.newLocal[Int]("nMissingBytes", pt.nMissingBytes(prefixLen))
          cb.ifx(nMissingLocal > 0, {
            cb += out.writeBytes(array + const(pt.lengthHeaderBytes), nMissingLocal - 1)
            cb += out.writeByte((Region.loadByte(array + const(pt.lengthHeaderBytes)
              + (nMissingLocal - 1).toL) & EType.lowBitMask(prefixLen)).toB)
          })
        }
      case _ =>
        val b = Code.newLocal[Int]("b")
        val shift = Code.newLocal[Int]("shift")
        cb.assign(i, 0)
        cb.assign(b, 0)
        cb.assign(shift, 0)
        cb.whileLoop(i < prefixLen, {
          cb.ifx(pt.isElementMissing(array, i), cb.assign(b, b | (const(1) << shift)))
          cb.assign(shift, shift + 1)
          cb.assign(i, i + 1)
          cb.ifx(shift.ceq(7), {
            cb.assign(shift, 0)
            cb += out.writeByte(b.toB)
            cb.assign(b, 0)
          })
        })
        cb.ifx(shift > 0, cb += out.writeByte(b.toB))
    }

    val writeElemF = elementType.buildEncoder(pt.elementType, cb.emb.ecb)
    cb.forLoop(cb.assign(i, 0), i < prefixLen, cb.assign(i, i + 1), {
      arr.loadElement(cb, i).consume(cb, { /* do nothing */ }, { pc =>
        cb += writeElemF(pc.code, out)
      })
    })
  }

  def _buildFundamentalEncoder(cb: EmitCodeBuilder, pt: PType, v: Value[_], out: Value[OutputBuffer]): Unit = {
    val pa = pt.asInstanceOf[PArray]
    val array = coerce[Long](v)
    buildPrefixEncoder(cb, pa, array, out, pa.loadLength(array))
  }

  def _buildFundamentalDecoder(
    pt: PType,
    mb: EmitMethodBuilder[_],
    region: Value[Region],
    in: Value[InputBuffer]
  ): Code[Long] = {
    val t = pt.asInstanceOf[PArray]
    val len = mb.newLocal[Int]("len")
    val i = mb.newLocal[Int]("i")
    val array = mb.newLocal[Long]("array")
    val readElemF = elementType.buildInplaceDecoder(t.elementType, mb.ecb)

    val readMissing: Code[Unit] = pt match {
      case t: PCanonicalArray if t.elementType.required == elementType.required =>
        if (elementType.required)
          Code._empty
        else in.readBytes(region, array + const(t.lengthHeaderBytes), t.nMissingBytes(len))
      case _ =>
        if (elementType.required) {
          EmitCodeBuilder.scopedVoid(mb) { cb =>
            cb.assign(i, 0)
            cb.whileLoop(i < len, {
              cb += t.setElementPresent(array, i)
              cb.assign(i, i + 1)
            })
          }
        } else {
          val missingBitsAddr = mb.newLocal[Long]("missingBitsAddr")
          EmitCodeBuilder.scopedVoid(mb) { cb =>
            cb.assign(missingBitsAddr, region.allocate(1L, t.nMissingBytes(len).toL))
            cb.assign(i, 0)
            cb += in.readBytes(region, missingBitsAddr, t.nMissingBytes(len))
            cb.whileLoop(i < len, {
              cb.ifx(Region.loadBit(missingBitsAddr, i.toL),
                cb += t.setElementMissing(array, i),
                cb += t.setElementPresent(array, i))
              cb.assign(i, i + 1)
            })
          }
        }
    }

    Code(
      len := in.readInt(),
      array := t.allocate(region, len),
      t.storeLength(array, len),
      readMissing,
      i := 0,
      Code.whileLoop(
        i < len,
        Code(
          if (elementType.required)
            readElemF(region, t.elementOffset(array, len, i), in)
          else
            t.isElementDefined(array, i).mux(
              readElemF(region, t.elementOffset(array, len, i), in),
              Code._empty),
          i := i + const(1))),
      array.load())
  }

  def _buildSkip(mb: EmitMethodBuilder[_], r: Value[Region], in: Value[InputBuffer]): Code[Unit] = {
    val len = mb.newLocal[Int]("len")
    val i = mb.newLocal[Int]("i")
    val skip = elementType.buildSkip(mb)

    if (elementType.required) {
      Code(
        len := in.readInt(),
        i := 0,
        Code.whileLoop(i < len,
          Code(
            skip(r, in),
            i := i + const(1))))
    } else {
      val mbytes = mb.newLocal[Long]("mbytes")
      val nMissing = mb.newLocal[Int]("nMissing")
      Code(
        len := in.readInt(),
        nMissing := UnsafeUtils.packBitsToBytes(len),
        mbytes := r.allocate(const(1), nMissing.toL),
        in.readBytes(r, mbytes, nMissing),
        i := 0,
        Code.whileLoop(i < len,
          Region.loadBit(mbytes, i.toL).mux(
            Code._empty,
            skip(r, in)),
          i := i + const(1)))
    }
  }

  def _asIdent = s"array_of_${elementType.asIdent}"
  def _toPretty = s"EArray[$elementType]"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean = false) {
    sb.append("EArray[")
    elementType.pretty(sb, indent, compact)
    sb.append("]")
  }

  def setRequired(newRequired: Boolean): EArray = EArray(elementType, newRequired)
}
