package is.hail.utils

import is.hail.annotations.{Annotation, RegionValueBuilder, UnsafeRow}
import is.hail.expr.types.TBaseStruct

import scala.collection.mutable

class MissingBaseStructArrayBuilder extends Serializable{
  private var len = 0
  private val elements = new ArrayBuilder[Annotation]()
  private val isMissing = new mutable.BitSet()

  def addMissing() {
    isMissing.add(len)
    len += 1
  }

  def add(x: Annotation) {
    elements += x
    len += 1
  }

  def length(): Int = len

  def foreach(whenMissing: (Int) => Unit)(whenPresent: (Int, Annotation) => Unit) {
    var i = 0
    var j = 0
    while (i < len) {
      if (isMissing(i))
        whenMissing(i)
      else {
        whenPresent(i, elements(j))
        j += 1
      }
      i += 1
    }
  }

  def write(rvb: RegionValueBuilder, t: TBaseStruct) {
    rvb.startArray(len)
    var i = 0
    var j = 0
    while (i < len) {
      if (isMissing(i))
        rvb.setMissing()
      else {
        rvb.addAnnotation(t, elements(j))
        j += 1
      }
      i += 1
    }
    rvb.endArray()
  }

  def clear() {
    len = 0
    elements.clear()
    isMissing.clear()
  }
}
