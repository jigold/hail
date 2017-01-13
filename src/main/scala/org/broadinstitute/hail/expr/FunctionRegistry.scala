package org.broadinstitute.hail.expr

import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.stats._
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.variant.{AltAllele, Genotype, Locus, Variant}
import org.broadinstitute.hail.methods._

import scala.collection.mutable
import org.broadinstitute.hail.utils.EitherIsAMonad._
import org.json4s.jackson.JsonMethods

import scala.language.higherKinds

object FunctionRegistry {

  sealed trait LookupError {
    def message: String
  }

  sealed case class NotFound(name: String, typ: TypeTag) extends LookupError {
    def message = s"No function found with name `$name' and argument ${ plural(typ.xs.size, "type") } $typ"
  }

  sealed case class Ambiguous(name: String, typ: TypeTag, alternates: Seq[(Int, (TypeTag, Fun))]) extends LookupError {
    def message =
      s"""found ${ alternates.size } ambiguous matches for $typ:
          |  ${ alternates.map(_._2._1).mkString("\n  ") }""".stripMargin
  }

  type Err[T] = Either[LookupError, T]

  def generateDocumentation(file: String) = {

    def methodToRst(name: String, tt: TypeTag, fun: Fun, docstring: Option[String]) = {
      val sb = new StringBuilder
      sb.append(s" - **$name")

      val argsType = tt.xs.drop(1).map(_.toString.replaceAll("\\?T", "T"))
      val retType = fun.retType.toString.replaceAll("\\?T", "T")

      if (argsType.nonEmpty) {
        sb.append("(")
        sb.append(argsType.mkString(", "))
        sb.append(")")
      }

      sb.append(s"**: *$retType*")

      docstring match {
        case Some(s) => sb.append(s" -- $s")
        case None =>
      }

      sb.result()
    }

    def functionToRst = ???

    registry.foreach{ case (name, funs) =>
      funs.foreach { case (tt, fun, docstring) => println {
        fun match {
          case f: OptionUnaryFun[_, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: UnaryFun[_, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity0Aggregator[_, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity1Aggregator[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity3Aggregator[_, _, _, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: UnaryLambdaAggregator[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: BinaryLambdaAggregator[_, _, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: UnarySpecial[_, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: BinaryFun[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: BinarySpecial[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: BinaryLambdaSpecial[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity3LambdaFun[_, _, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: BinaryLambdaSpecial[_, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity3Fun[_, _, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case f: Arity4Fun[_, _, _, _, _] =>
            ("method", tt.xs(0).toString.replaceAll("\\?T", "T"), methodToRst(name, tt, fun, docstring))
          case _ =>
        }
      }
      }
    }
  }

  private val registry = mutable.HashMap[String, Seq[(TypeTag, Fun, Option[String])]]().withDefaultValue(Seq.empty)

  private val conversions = new mutable.HashMap[(Type, Type), (Int, UnaryFun[Any, Any])]

  private def lookupConversion(from: Type, to: Type): Option[(Int, UnaryFun[Any, Any])] = conversions.get(from -> to)

  private def registerConversion[T, U](how: T => U, priority: Int = 1)(implicit hrt: HailRep[T], hru: HailRep[U]) {
    val from = hrt.typ
    val to = hru.typ
    require(priority >= 1)
    lookupConversion(from, to) match {
      case Some(_) =>
        throw new RuntimeException(s"The conversion between $from and $to is already bound")
      case None =>
        conversions.put(from -> to, priority -> UnaryFun[Any, Any](to, x => how(x.asInstanceOf[T])))
    }
  }

  private def lookup(name: String, typ: TypeTag): Err[Fun] = {

    val matches = registry(name).flatMap { case (tt, f, _) =>
      tt.clear()
      if (tt.xs.size == typ.xs.size) {
        val conversions = (tt.xs, typ.xs).zipped.map { case (l, r) =>
          if (l.isBound) {
            if (l.unify(r))
              Some(None)
            else {
              val conv = lookupConversion(r, l).map(c => Some(c))
              conv
            }
          } else if (l.unify(r)) {
            Some(None)
          } else
            None
        }

        anyFailAllFail[Array, Option[(Int, UnaryFun[Any, Any])]](conversions)
          .map { arr =>
            if (arr.forall(_.isEmpty))
              0 -> (tt.subst(), f.subst())
            else {
              val arr2 = arr.map(_.getOrElse(0 -> UnaryFun[Any, Any](null, (a: Any) => a)))
              arr2.map(_._1).max -> (tt.subst(), f.subst().convertArgs(arr2.map(_._2)))
            }
          }
      } else
        None
    }.groupBy(_._1).toArray.sortBy(_._1)

    matches.headOption
      .toRight[LookupError](NotFound(name, typ))
      .flatMap { case (priority, it) =>
        assert(it.nonEmpty)
        if (it.size == 1)
          Right(it.head._2._2)
        else {
          assert(priority != 0)
          Left(Ambiguous(name, typ, it))
        }
      }
  }

  private def bind(name: String, typ: TypeTag, f: Fun, docstring: Option[String]) = {
    registry.updateValue(name, Seq.empty, (typ, f, docstring) +: _)
  }

  def lookupMethodReturnType(typ: Type, typs: Seq[Type], name: String): Err[Type] =
    lookup(name, MethodType(typ +: typs: _*)).map(_.retType)

  def lookupMethod(ec: EvalContext)(typ: Type, typs: Seq[Type], name: String)(lhs: AST, args: Seq[AST]): Err[() => Any] = {
    require(typs.length == args.length)

    val m = lookup(name, MethodType(typ +: typs: _*))
    m.map {
      case aggregator: Arity0Aggregator[_, _] =>
        val localA = ec.a
        val idx = localA.length
        localA += null
        ec.aggregations += ((idx, lhs.eval(ec), aggregator.ctor()))
        () => localA(idx)

      case aggregator: Arity1Aggregator[_, u, _] =>
        val localA = ec.a
        val idx = localA.length
        localA += null

        val u = args(0).eval(EvalContext())()

        if (u == null)
          fatal("Argument evaluated to missing in call to aggregator $name")

        ec.aggregations += ((idx, lhs.eval(ec), aggregator.ctor(
          u.asInstanceOf[u])))
        () => localA(idx)

      case aggregator: Arity3Aggregator[_, u, v, w, _] =>
        val localA = ec.a
        val idx = localA.length
        localA += null

        val u = args(0).eval(EvalContext())()
        val v = args(1).eval(EvalContext())()
        val w = args(2).eval(EvalContext())()

        if (u == null)
          fatal("Argument 1 evaluated to missing in call to aggregator $name")
        if (v == null)
          fatal("Argument 2 evaluated to missing in call to aggregator $name")
        if (w == null)
          fatal("Argument 3 evaluated to missing in call to aggregator $name")

        ec.aggregations += ((idx, lhs.eval(ec), aggregator.ctor(
          u.asInstanceOf[u],
          v.asInstanceOf[v],
          w.asInstanceOf[w])))
        () => localA(idx)

      case aggregator: UnaryLambdaAggregator[t, u, v] =>
        val Lambda(_, param, body) = args(0)

        val idx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyST =
          lhs.`type` match {
            case tagg: TAggregable => tagg.symTab
            case _ => ec.st
          }

        val bodyFn = body.eval(ec.copy(st = bodyST + (param -> (idx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(idx) = x
          bodyFn()
        }

        ec.aggregations += ((idx, lhs.eval(ec), aggregator.ctor(g)))
        () => localA(idx)

      case aggregator: BinaryLambdaAggregator[t, u, v, w] =>
        val Lambda(_, param, body) = args(0)

        val idx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyST =
          lhs.`type` match {
            case tagg: TAggregable => tagg.symTab
            case _ => ec.st
          }

        val bodyFn = body.eval(ec.copy(st = bodyST + (param -> (idx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(idx) = x
          bodyFn()
        }

        val v = args(1).eval(EvalContext())()
        if (v == null)
          fatal("Argument evaluated to missing in call to aggregator $name")

        ec.aggregations += ((idx, lhs.eval(ec), aggregator.ctor(g, v.asInstanceOf[v])))
        () => localA(idx)

      case f: UnaryFun[_, _] =>
        AST.evalCompose(ec, lhs)(f)
      case f: UnarySpecial[_, _] =>
        val t = lhs.eval(ec)
        () => f(t)
      case f: OptionUnaryFun[_, _] =>
        AST.evalFlatCompose(ec, lhs)(f)
      case f: BinaryFun[_, _, _] =>
        AST.evalCompose(ec, lhs, args(0))(f)
      case f: BinarySpecial[_, _, _] =>
        val t = lhs.eval(ec)
        val u = args(0).eval(ec)
        () => f(t, u)
      case f: BinaryLambdaFun[t, _, _] =>
        val Lambda(_, param, body) = args(0)

        val localIdx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyST =
          lhs.`type` match {
            case tagg: TAggregable => tagg.symTab
            case _ => ec.st
          }

        val bodyFn = body.eval(ec.copy(st = bodyST + (param -> (localIdx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(localIdx) = x
          bodyFn()
        }

        AST.evalCompose[t](ec, lhs) { x1 => f(x1, g) }
      case f: Arity3LambdaFun[t, _, v, _] =>
        val Lambda(_, param, body) = args(0)

        val localIdx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyST =
          lhs.`type` match {
            case tagg: TAggregable => tagg.symTab
            case _ => ec.st
          }

        val bodyFn = body.eval(ec.copy(st = bodyST + (param -> (localIdx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(localIdx) = x
          bodyFn()
        }

        AST.evalCompose[t, v](ec, lhs, args(1)) { (x1, x2) => f(x1, g, x2) }
      case f: BinaryLambdaSpecial[t, _, _] =>
        val Lambda(_, param, body) = args(0)

        val idx = ec.a.length
        val localA = ec.a
        localA += null

        val bodyST =
          lhs.`type` match {
            case tagg: TAggregable => tagg.symTab
            case _ => ec.st
          }

        val bodyFn = body.eval(ec.copy(st = bodyST + (param -> (idx, lhs.`type`.asInstanceOf[TContainer].elementType))))
        val g = (x: Any) => {
          localA(idx) = x
          bodyFn()
        }

        val t = lhs.eval(ec)
        () => f(t, g)
      case f: Arity3Fun[_, _, _, _] =>
        AST.evalCompose(ec, lhs, args(0), args(1))(f)
      case f: Arity4Fun[_, _, _, _, _] =>
        AST.evalCompose(ec, lhs, args(0), args(1), args(2))(f)
      case fn =>
        throw new RuntimeException(s"Internal hail error, bad binding in function registry for `$name' with argument types $typ, $typs: $fn")
    }
  }

  def lookupFun(ec: EvalContext)(name: String, typs: Seq[Type])(args: Seq[AST]): Err[() => Any] = {
    require(typs.length == args.length)

    lookup(name, FunType(typs: _*)).map {
      case f: UnaryFun[_, _] =>
        AST.evalCompose(ec, args(0))(f)
      case f: UnarySpecial[_, _] =>
        val t = args(0).eval(ec)
        () => f(t)
      case f: OptionUnaryFun[_, _] =>
        AST.evalFlatCompose(ec, args(0))(f)
      case f: BinaryFun[_, _, _] =>
        AST.evalCompose(ec, args(0), args(1))(f)
      case f: BinarySpecial[_, _, _] =>
        val t = args(0).eval(ec)
        val u = args(1).eval(ec)
        () => f(t, u)
      case f: Arity3Fun[_, _, _, _] =>
        AST.evalCompose(ec, args(0), args(1), args(2))(f)
      case f: Arity4Fun[_, _, _, _, _] =>
        AST.evalCompose(ec, args(0), args(1), args(2), args(3))(f)
      case fn =>
        throw new RuntimeException(s"Internal hail error, bad binding in function registry for `$name' with argument types $typs: $fn")
    }
  }

  def lookupFunReturnType(name: String, typs: Seq[Type]): Err[Type] =
    lookup(name, FunType(typs: _*)).map(_.retType)

  def registerMethod[T, U](name: String, impl: T => U, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(hrt.typ), UnaryFun[T, U](hru.typ, impl), docstring)
  }

  def registerMethod[T, U, V](name: String, impl: (T, U) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, MethodType(hrt.typ, hru.typ), BinaryFun[T, U, V](hrv.typ, impl), docstring)
  }

  def registerMethodSpecial[T, U, V](name: String, impl: (() => Any, () => Any) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, MethodType(hrt.typ, hru.typ), BinarySpecial[T, U, V](hrv.typ, impl), docstring)
  }

  def registerLambdaMethod[T, U, V](name: String, impl: (T, (Any) => Any) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    val m = BinaryLambdaFun[T, U, V](hrv.typ, impl)
    bind(name, MethodType(hrt.typ, hru.typ), m, docstring)
  }

  def registerLambdaMethod[T, U, V, W](name: String, impl: (T, (Any) => Any, V) => W, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    val m = Arity3LambdaFun[T, U, V, W](hrw.typ, impl)
    bind(name, MethodType(hrt.typ, hru.typ, hrv.typ), m, docstring)
  }

  def registerLambdaSpecial[T, U, V](name: String, impl: (() => Any, (Any) => Any) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    val m = BinaryLambdaSpecial[T, U, V](hrv.typ, impl)
    bind(name, MethodType(hrt.typ, hru.typ), m, docstring)
  }

  def registerMethod[T, U, V, W](name: String, impl: (T, U, V) => W, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    bind(name, MethodType(hrt.typ, hru.typ, hrv.typ), Arity3Fun[T, U, V, W](hrw.typ, impl), docstring)
  }

  def register[T, U](name: String, impl: T => U, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, FunType(hrt.typ), UnaryFun[T, U](hru.typ, impl), docstring)
  }

  def registerSpecial[T, U](name: String, impl: (() => Any) => U, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, FunType(hrt.typ), UnarySpecial[T, U](hru.typ, impl), docstring)
  }

  def registerOptionMethod[T, U](name: String, impl: T => Option[U], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(hrt.typ), OptionUnaryFun[T, U](hru.typ, impl), docstring)
  }

  def registerOption[T, U](name: String, impl: T => Option[U], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, FunType(hrt.typ), OptionUnaryFun[T, U](hru.typ, impl), docstring)
  }

  def registerUnaryNAFilteredCollectionMethod[T, U](name: String, impl: TraversableOnce[T] => U, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(TArray(hrt.typ)), UnaryFun[IndexedSeq[_], U](hru.typ, { (ts: IndexedSeq[_]) =>
      impl(ts.filter(t => t != null).map(_.asInstanceOf[T]))
    }), docstring)
    bind(name, MethodType(TSet(hrt.typ)), UnaryFun[Set[_], U](hru.typ, { (ts: Set[_]) =>
      impl(ts.filter(t => t != null).map(_.asInstanceOf[T]))
    }), docstring)
  }

  def register[T, U, V](name: String, impl: (T, U) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, FunType(hrt.typ, hru.typ), BinaryFun[T, U, V](hrv.typ, impl), docstring)
  }

  def registerSpecial[T, U, V](name: String, impl: (() => Any, () => Any) => V, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, FunType(hrt.typ, hru.typ), BinarySpecial[T, U, V](hrv.typ, impl), docstring)
  }

  def register[T, U, V, W](name: String, impl: (T, U, V) => W, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    bind(name, FunType(hrt.typ, hru.typ, hrv.typ), Arity3Fun[T, U, V, W](hrw.typ, impl), docstring)
  }

  def register[T, U, V, W, X](name: String, impl: (T, U, V, W) => X, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W], hrx: HailRep[X]) = {
    bind(name, FunType(hrt.typ, hru.typ, hrv.typ, hrw.typ), Arity4Fun[T, U, V, W, X](hrx.typ, impl), docstring)
  }

  def registerAnn[T](name: String, t: TStruct, impl: T => Annotation, docstring: Option[String])
    (implicit hrt: HailRep[T]) = {
    register(name, impl, docstring)(hrt, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U](name: String, t: TStruct, impl: (T, U) => Annotation, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    register(name, impl, docstring)(hrt, hru, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U, V](name: String, t: TStruct, impl: (T, U, V) => Annotation, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    register(name, impl, docstring)(hrt, hru, hrv, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAnn[T, U, V, W](name: String, t: TStruct, impl: (T, U, V, W) => Annotation, docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    register(name, impl, docstring)(hrt, hru, hrv, hrw, new HailRep[Annotation] {
      def typ = t
    })
  }

  def registerAggregator[T, U](name: String, ctor: () => TypedAggregator[U], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U]) = {
    bind(name, MethodType(hrt.typ), Arity0Aggregator[T, U](hru.typ, ctor), docstring)
  }

  def registerLambdaAggregator[T, U, V](name: String, ctor: ((Any) => Any) => TypedAggregator[V], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, MethodType(hrt.typ, hru.typ), UnaryLambdaAggregator[T, U, V](hrv.typ, ctor), docstring)
  }

  def registerLambdaAggregator[T, U, V, W](name: String, ctor: ((Any) => Any, V) => TypedAggregator[W], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W]) = {
    bind(name, MethodType(hrt.typ, hru.typ, hrv.typ), BinaryLambdaAggregator[T, U, V, W](hrw.typ, ctor), docstring)
  }

  def registerAggregator[T, U, V](name: String, ctor: (U) => TypedAggregator[V], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V]) = {
    bind(name, MethodType(hrt.typ, hru.typ), Arity1Aggregator[T, U, V](hrv.typ, ctor), docstring)
  }

  def registerAggregator[T, U, V, W, X](name: String, ctor: (U, V, W) => TypedAggregator[X], docstring: Option[String])
    (implicit hrt: HailRep[T], hru: HailRep[U], hrv: HailRep[V], hrw: HailRep[W], hrx: HailRep[X]) = {
    bind(name, MethodType(hrt.typ, hru.typ, hrv.typ, hrw.typ), Arity3Aggregator[T, U, V, W, X](hrx.typ, ctor), docstring)
  }

  val TT = TVariable("T")
  val TU = TVariable("U")
  val TV = TVariable("V")

  val TTHr = new HailRep[Any] {
    def typ = TT
  }
  val TUHr = new HailRep[Any] {
    def typ = TU
  }
  val TVHr = new HailRep[Any] {
    def typ = TV
  }

  registerOptionMethod("gt", { (x: Genotype) => x.gt },
    Option("the call, gt = ``k*(k+1)/2+j`` for call ``j/k``"))
  registerOptionMethod("gtj", { (x: Genotype) => x.gt.map(gtx => Genotype.gtPair(gtx).j) }, None)
  registerOptionMethod("gtk", { (x: Genotype) => x.gt.map(gtx => Genotype.gtPair(gtx).k) }, None)
  registerOptionMethod("ad", { (x: Genotype) => x.ad.map(a => a: IndexedSeq[Int]) }, None)
  registerOptionMethod("dp", { (x: Genotype) => x.dp }, None)
  registerOptionMethod("od", { (x: Genotype) => x.od }, None)
  registerOptionMethod("gq", { (x: Genotype) => x.gq }, None)
  registerOptionMethod("pl", { (x: Genotype) => x.pl.map(a => a: IndexedSeq[Int]) }, None)
  registerOptionMethod("dosage", { (x: Genotype) => x.dosage.map(a => a: IndexedSeq[Double]) }, None)
  registerMethod("isHomRef", { (x: Genotype) => x.isHomRef }, None)
  registerMethod("isHet", { (x: Genotype) => x.isHet }, None)
  registerMethod("isHomVar", { (x: Genotype) => x.isHomVar }, None)
  registerMethod("isCalledNonRef", { (x: Genotype) => x.isCalledNonRef }, None)
  registerMethod("isHetNonRef", { (x: Genotype) => x.isHetNonRef }, None)
  registerMethod("isHetRef", { (x: Genotype) => x.isHetRef }, None)
  registerMethod("isCalled", { (x: Genotype) => x.isCalled }, None)
  registerMethod("isNotCalled", { (x: Genotype) => x.isNotCalled }, None)
  registerOptionMethod("nNonRefAlleles", { (x: Genotype) => x.nNonRefAlleles }, None)
  registerOptionMethod("pAB", { (x: Genotype) => x.pAB() }, None)
  registerOptionMethod("fractionReadsRef", { (x: Genotype) => x.fractionReadsRef() }, None)
  registerMethod("fakeRef", { (x: Genotype) => x.fakeRef }, None)
  registerMethod("isDosage", { (x: Genotype) => x.isDosage }, None)
  registerMethod("contig", { (x: Variant) => x.contig }, None)
  registerMethod("start", { (x: Variant) => x.start }, None)
  registerMethod("ref", { (x: Variant) => x.ref }, None)
  registerMethod("altAlleles", { (x: Variant) => x.altAlleles }, None)
  registerMethod("nAltAlleles", { (x: Variant) => x.nAltAlleles }, None)
  registerMethod("nAlleles", { (x: Variant) => x.nAlleles }, None)
  registerMethod("isBiallelic", { (x: Variant) => x.isBiallelic }, None)
  registerMethod("nGenotypes", { (x: Variant) => x.nGenotypes }, None)
  registerMethod("inXPar", { (x: Variant) => x.inXPar }, None)
  registerMethod("inYPar", { (x: Variant) => x.inYPar }, None)
  registerMethod("inXNonPar", { (x: Variant) => x.inXNonPar }, None)
  registerMethod("inYNonPar", { (x: Variant) => x.inYNonPar }, None)
  // assumes biallelic
  registerMethod("alt", { (x: Variant) => x.alt }, None)
  registerMethod("altAllele", { (x: Variant) => x.altAllele }, None)
  registerMethod("locus", { (x: Variant) => x.locus }, None)
  registerMethod("contig", { (x: Locus) => x.contig }, None)
  registerMethod("position", { (x: Locus) => x.position }, None)
  registerMethod("start", { (x: Interval[Locus]) => x.start }, None)
  registerMethod("end", { (x: Interval[Locus]) => x.end }, None)
  registerMethod("ref", { (x: AltAllele) => x.ref }, None)
  registerMethod("alt", { (x: AltAllele) => x.alt }, None)
  registerMethod("isSNP", { (x: AltAllele) => x.isSNP }, None)
  registerMethod("isMNP", { (x: AltAllele) => x.isMNP }, None)
  registerMethod("isIndel", { (x: AltAllele) => x.isIndel }, None)
  registerMethod("isInsertion", { (x: AltAllele) => x.isInsertion }, None)
  registerMethod("isDeletion", { (x: AltAllele) => x.isDeletion }, None)
  registerMethod("isComplex", { (x: AltAllele) => x.isComplex }, None)
  registerMethod("isTransition", { (x: AltAllele) => x.isTransition }, None)
  registerMethod("isTransversion", { (x: AltAllele) => x.isTransversion }, None)
  registerMethod("isAutosomal", { (x: Variant) => x.isAutosomal }, None)

  registerMethod("length", { (x: String) => x.length }, None)

  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Int]) => x.sum }, None)
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Long]) => x.sum }, None)
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Float]) => x.sum }, None)
  registerUnaryNAFilteredCollectionMethod("sum", { (x: TraversableOnce[Double]) => x.sum }, None)

  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Int]) => x.min }, None)
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Long]) => x.min }, None)
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Float]) => x.min }, None)
  registerUnaryNAFilteredCollectionMethod("min", { (x: TraversableOnce[Double]) => x.min }, None)

  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Int]) => x.max }, None)
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Long]) => x.max }, None)
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Float]) => x.max }, None)
  registerUnaryNAFilteredCollectionMethod("max", { (x: TraversableOnce[Double]) => x.max }, None)

  register("range", { (x: Int) => 0 until x: IndexedSeq[Int] }, None)
  register("range", { (x: Int, y: Int) => x until y: IndexedSeq[Int] }, None)
  register("range", { (x: Int, y: Int, step: Int) => x until y by step: IndexedSeq[Int] }, None)
  register("Variant", { (x: String) =>
    val Array(chr, pos, ref, alts) = x.split(":")
    Variant(chr, pos.toInt, ref, alts.split(","))
  }, None)
  register("Variant", { (x: String, y: Int, z: String, a: String) => Variant(x, y, z, a) }, None)
  register("Variant", { (x: String, y: Int, z: String, a: IndexedSeq[String]) => Variant(x, y, z, a.toArray) }, None)

  register("Locus", { (x: String) =>
    val Array(chr, pos) = x.split(":")
    Locus(chr, pos.toInt)
  }, None)
  register("Locus", { (x: String, y: Int) => Locus(x, y) }, None)
  register("Interval", { (x: Locus, y: Locus) => Interval(x, y) }, None)
  registerAnn("hwe", TStruct(("rExpectedHetFrequency", TDouble), ("pHWE", TDouble)), { (nHomRef: Int, nHet: Int, nHomVar: Int) =>
    if (nHomRef < 0 || nHet < 0 || nHomVar < 0)
      fatal(s"got invalid (negative) argument to function `hwe': hwe($nHomRef, $nHet, $nHomVar)")
    val n = nHomRef + nHet + nHomVar
    val nAB = nHet
    val nA = nAB + 2 * nHomRef.min(nHomVar)

    val LH = LeveneHaldane(n, nA)
    Annotation(divOption(LH.getNumericalMean, n).orNull, LH.exactMidP(nAB))
  }, None)
  registerAnn("fet", TStruct(("pValue", TDouble), ("oddsRatio", TDouble), ("ci95Lower", TDouble), ("ci95Upper", TDouble)), { (c1: Int, c2: Int, c3: Int, c4: Int) =>
    if (c1 < 0 || c2 < 0 || c3 < 0 || c4 < 0)
      fatal(s"got invalid argument to function `fet': fet($c1, $c2, $c3, $c4)")
    val fet = FisherExactTest(c1, c2, c3, c4)
    Annotation(fet(0).orNull, fet(1).orNull, fet(2).orNull, fet(3).orNull)
  }, None)
  // NB: merge takes two structs, how do I deal with structs?
  register("exp", { (x: Double) => math.exp(x) }, None)
  register("log10", { (x: Double) => math.log10(x) }, None)
  register("sqrt", { (x: Double) => math.sqrt(x) }, None)
  register("log", (x: Double) => math.log(x), None)
  register("log", (x: Double, b: Double) => math.log(x) / math.log(b), None)
  register("pow", (b: Double, x: Double) => math.pow(b, x), None)

  register("pcoin", { (p: Double) => math.random < p }, None)
  register("runif", { (min: Double, max: Double) => min + (max - min) * math.random }, None)
  register("rnorm", { (mean: Double, sd: Double) => mean + sd * scala.util.Random.nextGaussian() }, None)

  register("pnorm", { (x: Double) => pnorm(x) }, None)
  register("qnorm", { (p: Double) => qnorm(p) }, None)

  register("pchisq1tail", { (x: Double) => chiSquaredTail(1.0, x) }, None)
  register("qchisq1tail", { (p: Double) => inverseChiSquaredTail(1.0, p) }, None)

  register("!", (a: Boolean) => !a, None)

  registerConversion((x: Int) => x.toDouble, priority = 2)
  registerConversion { (x: Long) => x.toDouble }
  registerConversion { (x: Int) => x.toLong }
  registerConversion { (x: Float) => x.toDouble }

  registerConversion((x: IndexedSeq[Any]) => x.map { xi =>
    if (xi == null)
      null
    else
      xi.asInstanceOf[Int].toDouble
  }, priority = 2)(arrayHr(boxedintHr), arrayHr(boxeddoubleHr))

  registerConversion((x: IndexedSeq[Any]) => x.map { xi =>
    if (xi == null)
      null
    else
      xi.asInstanceOf[Long].toDouble
  })(arrayHr(boxedlongHr), arrayHr(boxeddoubleHr))

  registerConversion((x: IndexedSeq[Any]) => x.map { xi =>
    if (xi == null)
      null
    else
      xi.asInstanceOf[Int].toLong
  })(arrayHr(boxedintHr), arrayHr(boxedlongHr))

  registerConversion((x: IndexedSeq[Any]) => x.map { xi =>
    if (xi == null)
      null
    else
      xi.asInstanceOf[Float].toDouble
  })(arrayHr(boxedfloatHr), arrayHr(boxeddoubleHr))

  register("gtj", (i: Int) => Genotype.gtPair(i).j, None)
  register("gtk", (i: Int) => Genotype.gtPair(i).k, None)
  register("gtIndex", (j: Int, k: Int) => Genotype.gtIndex(j, k), None)

  registerConversion((x: Any) =>
    if (x != null)
      x.asInstanceOf[Int].toDouble
    else
      null, priority = 2)(aggregableHr(boxedintHr), aggregableHr(boxeddoubleHr))
  registerConversion { (x: Any) =>
    if (x != null)
      x.asInstanceOf[Long].toDouble
    else
      null
  }(aggregableHr(boxedlongHr), aggregableHr(boxeddoubleHr))

  registerConversion { (x: Any) =>
    if (x != null)
      x.asInstanceOf[Int].toLong
    else
      null
  }(aggregableHr(boxedintHr), aggregableHr(boxedlongHr))

  registerConversion { (x: Any) =>
    if (x != null)
      x.asInstanceOf[Float].toDouble
    else
      null
  }(aggregableHr(boxedfloatHr), aggregableHr(boxeddoubleHr))

  registerMethod("split", (s: String, p: String) => s.split(p): IndexedSeq[String], None)

  registerMethod("oneHotAlleles", (g: Genotype, v: Variant) => g.oneHotAlleles(v).orNull, None)

  registerMethod("oneHotGenotype", (g: Genotype, v: Variant) => g.oneHotGenotype(v).orNull, None)

  registerMethod("replace", (str: String, pattern1: String, pattern2: String) =>
    str.replaceAll(pattern1, pattern2), None)

  registerMethod("contains", (interval: Interval[Locus], locus: Locus) => interval.contains(locus), None)

  registerMethod("length", (a: IndexedSeq[Any]) => a.length, None)(arrayHr(TTHr), intHr)
  registerMethod("size", (a: IndexedSeq[Any]) => a.size, None)(arrayHr(TTHr), intHr)
  registerMethod("size", (s: Set[Any]) => s.size, None)(setHr(TTHr), intHr)
  registerMethod("size", (d: Map[String, Any]) => d.size, None)(dictHr(TTHr), intHr)

  registerMethod("id", (s: String) => s, None)(sampleHr, stringHr)

  registerMethod("isEmpty", (a: IndexedSeq[Any]) => a.isEmpty, None)(arrayHr(TTHr), boolHr)
  registerMethod("isEmpty", (s: Set[Any]) => s.isEmpty, None)(setHr(TTHr), boolHr)
  registerMethod("isEmpty", (d: Map[String, Any]) => d.isEmpty, None)(dictHr(TTHr), boolHr)

  registerMethod("toSet", (a: IndexedSeq[Any]) => a.toSet, None)(arrayHr(TTHr), setHr(TTHr))
  registerMethod("toSet", (a: Set[Any]) => a, None)(setHr(TTHr), setHr(TTHr))
  registerMethod("toArray", (a: Set[Any]) => a.toArray[Any]: IndexedSeq[Any], None)(setHr(TTHr), arrayHr(TTHr))
  registerMethod("toArray", (a: IndexedSeq[Any]) => a, None)(arrayHr(TTHr), arrayHr(TTHr))

  registerMethod("head", (a: IndexedSeq[Any]) => a.head, None)(arrayHr(TTHr), TTHr)
  registerMethod("tail", (a: IndexedSeq[Any]) => a.tail, None)(arrayHr(TTHr), arrayHr(TTHr))

  registerMethod("head", (a: Set[Any]) => a.head, None)(setHr(TTHr), TTHr)
  registerMethod("tail", (a: Set[Any]) => a.tail, None)(setHr(TTHr), setHr(TTHr))

  registerMethod("flatten", (a: IndexedSeq[IndexedSeq[Any]]) =>
    flattenOrNull[IndexedSeq, Any](IndexedSeq.newBuilder[Any], a), None
  )(arrayHr(arrayHr(TTHr)), arrayHr(TTHr))

  registerMethod("flatten", (s: Set[Set[Any]]) =>
    flattenOrNull[Set, Any](Set.newBuilder[Any], s), None
  )(setHr(setHr(TTHr)), setHr(TTHr))

  registerMethod("mkString", (a: IndexedSeq[String], d: String) => a.mkString(d), None)(
    arrayHr(stringHr), stringHr, stringHr)
  registerMethod("mkString", (s: Set[String], d: String) => s.mkString(d), None)(
    setHr(stringHr), stringHr, stringHr)

  registerMethod("contains", (s: Set[Any], x: Any) => s.contains(x), None)(setHr(TTHr), TTHr, boolHr)
  registerMethod("contains", (d: Map[String, Any], x: String) => d.contains(x), None)(dictHr(TTHr), stringHr, boolHr)

  registerLambdaMethod("find", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.find { elt =>
      val r = f(elt)
      r != null && r.asInstanceOf[Boolean]
    }.orNull, None
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), TTHr)

  registerLambdaMethod("find", (s: Set[Any], f: (Any) => Any) =>
    s.find { elt =>
      val r = f(elt)
      r != null && r.asInstanceOf[Boolean]
    }.orNull, None
  )(setHr(TTHr), unaryHr(TTHr, boolHr), TTHr)

  registerLambdaMethod("map", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.map(f), None
  )(arrayHr(TTHr), unaryHr(TTHr, TUHr), arrayHr(TUHr))

  registerLambdaMethod("map", (s: Set[Any], f: (Any) => Any) =>
    s.map(f), None
  )(setHr(TTHr), unaryHr(TTHr, TUHr), setHr(TUHr))

  registerLambdaMethod("mapValues", (a: Map[String, Any], f: (Any) => Any) =>
    a.map { case (k, v) => (k, f(v)) }, None
  )(dictHr(TTHr), unaryHr(TTHr, TUHr), dictHr(TUHr))

  registerLambdaMethod("flatMap", (a: IndexedSeq[Any], f: (Any) => Any) =>
    flattenOrNull[IndexedSeq, Any](IndexedSeq.newBuilder[Any],
      a.map(f).asInstanceOf[IndexedSeq[IndexedSeq[Any]]]), None
  )(arrayHr(TTHr), unaryHr(TTHr, arrayHr(TUHr)), arrayHr(TUHr))

  registerLambdaMethod("flatMap", (s: Set[Any], f: (Any) => Any) =>
    flattenOrNull[Set, Any](Set.newBuilder[Any],
      s.map(f).asInstanceOf[Set[Set[Any]]]), None
  )(setHr(TTHr), unaryHr(TTHr, setHr(TUHr)), setHr(TUHr))

  registerLambdaMethod("exists", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.exists { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("exists", (s: Set[Any], f: (Any) => Any) =>
    s.exists { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(setHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("forall", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.forall { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("forall", (s: Set[Any], f: (Any) => Any) =>
    s.forall { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(setHr(TTHr), unaryHr(TTHr, boolHr), boolHr)

  registerLambdaMethod("filter", (a: IndexedSeq[Any], f: (Any) => Any) =>
    a.filter { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(arrayHr(TTHr), unaryHr(TTHr, boolHr), arrayHr(TTHr))

  registerLambdaMethod("filter", (s: Set[Any], f: (Any) => Any) =>
    s.filter { x =>
      val r = f(x)
      r != null && r.asInstanceOf[Boolean]
    }, None
  )(setHr(TTHr), unaryHr(TTHr, boolHr), setHr(TTHr))

  registerAggregator[Any, Long]("count", () => new CountAggregator(), None)(aggregableHr(TTHr), longHr)

  registerAggregator[Any, IndexedSeq[Any]]("collect", () => new CollectAggregator(), None)(aggregableHr(TTHr), arrayHr(TTHr))

  registerAggregator[Int, Int]("sum", () => new SumAggregator[Int](), None)(aggregableHr(intHr), intHr)

  registerAggregator[Long, Long]("sum", () => new SumAggregator[Long](), None)(aggregableHr(longHr), longHr)

  registerAggregator[Float, Float]("sum", () => new SumAggregator[Float](), None)(aggregableHr(floatHr), floatHr)

  registerAggregator[Double, Double]("sum", () => new SumAggregator[Double](), None)(aggregableHr(doubleHr), doubleHr)

  registerAggregator[IndexedSeq[Int], IndexedSeq[Int]]("sum", () => new SumArrayAggregator[Int](), None)(aggregableHr(arrayHr(intHr)), arrayHr(intHr))

  registerAggregator[IndexedSeq[Long], IndexedSeq[Long]]("sum", () => new SumArrayAggregator[Long](), None)(aggregableHr(arrayHr(longHr)), arrayHr(longHr))

  registerAggregator[IndexedSeq[Float], IndexedSeq[Float]]("sum", () => new SumArrayAggregator[Float](), None)(aggregableHr(arrayHr(floatHr)), arrayHr(floatHr))

  registerAggregator[IndexedSeq[Double], IndexedSeq[Double]]("sum", () => new SumArrayAggregator[Double](), None)(aggregableHr(arrayHr(doubleHr)), arrayHr(doubleHr))

  registerAggregator[Genotype, Any]("infoScore", () => new InfoScoreAggregator(), None)(aggregableHr(genotypeHr),
    new HailRep[Any] {
      def typ = InfoScoreCombiner.signature
    })

  registerAggregator[Genotype, Any]("hardyWeinberg", () => new HWEAggregator(), None)(aggregableHr(genotypeHr),
    new HailRep[Any] {
      def typ = HWECombiner.signature
    })

  registerAggregator[Any, Any]("counter", () => new CounterAggregator(), None)(aggregableHr(TTHr),
    new HailRep[Any] {
      def typ = TArray(TStruct("key" -> TTHr.typ, "count" -> TLong))
    })

  registerAggregator[Double, Any]("stats", () => new StatAggregator(), None)(aggregableHr(doubleHr),
    new HailRep[Any] {
      def typ = TStruct(("mean", TDouble), ("stdev", TDouble), ("min", TDouble),
        ("max", TDouble), ("nNotMissing", TLong), ("sum", TDouble))
    })

  registerAggregator[Double, Double, Double, Int, Any]("hist", (start: Double, end: Double, bins: Int) => {
    if (bins <= 0)
      fatal(s"""method `hist' expects `bins' argument to be > 0, but got $bins""")

    val binSize = (end - start) / bins
    if (binSize <= 0)
      fatal(
        s"""invalid bin size from given arguments (start = $start, end = $end, bins = $bins)
            |  Method requires positive bin size [(end - start) / bins], but got ${ binSize.formatted("%.2f") }
                  """.stripMargin)

    val indices = Array.tabulate(bins + 1)(i => start + i * binSize)

    new HistAggregator(indices)
  }, None)(aggregableHr(doubleHr), doubleHr, doubleHr, intHr, new HailRep[Any] {
    def typ = HistogramCombiner.schema
  })

  registerLambdaAggregator[Genotype, (Any) => Any, Any]("callStats", (vf: (Any) => Any) => new CallStatsAggregator(vf), None)(
    aggregableHr(genotypeHr), unaryHr(genotypeHr, variantHr), new HailRep[Any] {
      def typ = CallStats.schema
    })

  registerLambdaAggregator[Genotype, (Any) => Any, Any]("inbreeding", (af: (Any) => Any) => new InbreedingAggregator(af), None)(
    aggregableHr(genotypeHr), unaryHr(genotypeHr, doubleHr), new HailRep[Any] {
      def typ = InbreedingCombiner.signature
    })

  registerLambdaAggregator[Any, (Any) => Any, Any]("fraction", (f: (Any) => Any) => new FractionAggregator(f), None)(
    aggregableHr(TTHr), unaryHr(TTHr, boxedboolHr), boxeddoubleHr)

  registerAggregator("take", (n: Int) => new TakeAggregator(n), None)(
    aggregableHr(TTHr), intHr, arrayHr(TTHr))

  registerLambdaAggregator("takeBy", (f: (Any) => Any, n: Int) => new TakeByAggregator[Int](f, n), None)(
    aggregableHr(TTHr), unaryHr(TTHr, boxedintHr), intHr, arrayHr(TTHr))
  registerLambdaAggregator("takeBy", (f: (Any) => Any, n: Int) => new TakeByAggregator[Long](f, n), None)(
    aggregableHr(TTHr), unaryHr(TTHr, boxedlongHr), intHr, arrayHr(TTHr))
  registerLambdaAggregator("takeBy", (f: (Any) => Any, n: Int) => new TakeByAggregator[Float](f, n), None)(
    aggregableHr(TTHr), unaryHr(TTHr, boxedfloatHr), intHr, arrayHr(TTHr))
  registerLambdaAggregator("takeBy", (f: (Any) => Any, n: Int) => new TakeByAggregator[Double](f, n), None)(
    aggregableHr(TTHr), unaryHr(TTHr, boxeddoubleHr), intHr, arrayHr(TTHr))
  registerLambdaAggregator("takeBy", (f: (Any) => Any, n: Int) => new TakeByAggregator[String](f, n), None)(
    aggregableHr(TTHr), unaryHr(TTHr, stringHr), intHr, arrayHr(TTHr))

  val aggST = Box[SymbolTable]()

  registerLambdaSpecial("filter", { (a: () => Any, f: (Any) => Any) =>
    val x = a()
    val r = f(x)
    if (r != null && r.asInstanceOf[Boolean])
      x
    else
      null
  }, None)(aggregableHr(TTHr, aggST), unaryHr(TTHr, boolHr), aggregableHr(TTHr, aggST))

  registerLambdaSpecial("map", { (a: () => Any, f: (Any) => Any) =>
    f(a())
  }, None)(aggregableHr(TTHr, aggST), unaryHr(TTHr, TUHr), aggregableHr(TUHr, aggST))

  type Id[T] = T

  def registerNumeric[T, S](name: String, f: (T, T) => S)(implicit hrt: HailRep[T], hrs: HailRep[S]) {
    val hrboxedt = new HailRep[Any] {
      def typ: Type = hrt.typ
    }
    val hrboxeds = new HailRep[Any] {
      def typ: Type = hrs.typ
    }

    register(name, f, None)

    register(name, (x: IndexedSeq[Any], y: T) =>
      x.map { xi =>
        if (xi == null)
          null
        else
          f(xi.asInstanceOf[T], y)
      }, None)(arrayHr(hrboxedt), hrt, arrayHr(hrboxeds))

    register(name, (x: T, y: IndexedSeq[Any]) => y.map { yi =>
      if (yi == null)
        null
      else
        f(x, yi.asInstanceOf[T])
    }, None)(hrt, arrayHr(hrboxedt), arrayHr(hrboxeds))

    register(name, { (x: IndexedSeq[Any], y: IndexedSeq[Any]) =>
      if (x.length != y.length) fatal(
        s"""Cannot apply operation $name to arrays of unequal length:
            |  Left: ${ x.length } elements
            |  Right: ${ y.length } elements""".stripMargin)
      (x, y).zipped.map { case (xi, yi) =>
        if (xi == null || yi == null)
          null
        else
          f(xi.asInstanceOf[T], yi.asInstanceOf[T])
      }
    }, None)(arrayHr(hrboxedt), arrayHr(hrboxedt), arrayHr(hrboxeds))
  }

  registerMethod("toInt", (s: String) => s.toInt, None)
  registerMethod("toLong", (s: String) => s.toLong, None)
  registerMethod("toFloat", (s: String) => s.toFloat, None)
  registerMethod("toDouble", (s: String) => s.toDouble, None)

  registerMethod("toInt", (b: Boolean) => b.toInt, None)
  registerMethod("toLong", (b: Boolean) => b.toLong, None)
  registerMethod("toFloat", (b: Boolean) => b.toFloat, None)
  registerMethod("toDouble", (b: Boolean) => b.toDouble, None)

  def registerNumericType[T]()(implicit ev: Numeric[T], hrt: HailRep[T]) {
    registerNumeric("+", ev.plus)
    registerNumeric("-", ev.minus)
    registerNumeric("*", ev.times)
    registerNumeric("/", (x: T, y: T) => ev.toDouble(x) / ev.toDouble(y))

    registerMethod("abs", ev.abs _, None)
    registerMethod("signum", ev.signum _, None)

    register("-", ev.negate _, None)
    register("fromInt", ev.fromInt _, None)

    registerMethod("toInt", ev.toInt _, None)
    registerMethod("toLong", ev.toLong _, None)
    registerMethod("toFloat", ev.toFloat _, None)
    registerMethod("toDouble", ev.toDouble _, None)
  }

  registerNumericType[Int]()
  registerNumericType[Long]()
  registerNumericType[Float]()
  registerNumericType[Double]()

  register("==", (a: Any, b: Any) => a == b, None)(TTHr, TUHr, boolHr)
  register("!=", (a: Any, b: Any) => a != b, None)(TTHr, TUHr, boolHr)

  def registerOrderedType[T]()(implicit ord: Ordering[T], hrt: HailRep[T]) {
    val hrboxedt = new HailRep[Any] {
      def typ: Type = hrt.typ
    }

    register("<", ord.lt _, None)
    register("<=", ord.lteq _, None)
    register(">", ord.gt _, None)
    register(">=", ord.gteq _, None)

    registerMethod("min", ord.min _, None)
    registerMethod("max", ord.max _, None)

    registerMethod("sort", (a: IndexedSeq[Any]) => a.sorted(extendOrderingToNull(ord)), None)(arrayHr(hrboxedt), arrayHr(hrboxedt))
    registerMethod("sort", (a: IndexedSeq[Any], ascending: Boolean) =>
      a.sorted(extendOrderingToNull(
        if (ascending)
          ord
        else
          ord.reverse)), None
    )(arrayHr(hrboxedt), boolHr, arrayHr(hrboxedt))

    registerLambdaMethod("sortBy", (a: IndexedSeq[Any], f: (Any) => Any) =>
      a.sortBy(f)(extendOrderingToNull(ord)), None
    )(arrayHr(TTHr), unaryHr(TTHr, hrboxedt), arrayHr(TTHr))

    registerLambdaMethod("sortBy", (a: IndexedSeq[Any], f: (Any) => Any, ascending: Boolean) =>
      a.sortBy(f)(extendOrderingToNull(
        if (ascending)
          ord
        else
          ord.reverse)), None
    )(arrayHr(TTHr), unaryHr(TTHr, hrboxedt), boolHr, arrayHr(TTHr))
  }

  registerOrderedType[Boolean]()
  registerOrderedType[Int]()
  registerOrderedType[Long]()
  registerOrderedType[Float]()
  registerOrderedType[Double]()
  registerOrderedType[String]()

  registerMethod("//", (x: Int, y: Int) => java.lang.Math.floorDiv(x, y), None)
  registerMethod("//", (x: Long, y: Long) => java.lang.Math.floorDiv(x, y), None)
  registerMethod("//", (x: Float, y: Float) => math.floor(x / y).toFloat, None)
  registerMethod("//", (x: Double, y: Double) => math.floor(x / y), None)

  register("%", (x: Int, y: Int) => java.lang.Math.floorMod(x, y), None)
  register("%", (x: Long, y: Long) => java.lang.Math.floorMod(x,y), None)
  register("%", (x: Float, y: Float) => { val t = x % y; if (x >= 0 && y > 0 || x <= 0 && y < 0 || t == 0) t else t + y }, None)
  register("%", (x: Double, y: Double) => { val t = x % y; if (x >= 0 && y > 0 || x <= 0 && y < 0 || t == 0) t else t + y }, None)

  register("+", (x: String, y: Any) => x + y, None)(stringHr, TTHr, stringHr)

  register("~", (s: String, t: String) => s.r.findFirstIn(t).isDefined, None)

  registerSpecial("isMissing", (g: () => Any) => g() == null, None)(TTHr, boolHr)
  registerSpecial("isDefined", (g: () => Any) => g() != null, None)(TTHr, boolHr)

  registerSpecial("||", { (f1: () => Any, f2: () => Any) =>
    val x1 = f1()
    if (x1 != null) {
      if (x1.asInstanceOf[Boolean])
        true
      else
        f2()
    } else {
      val x2 = f2()
      if (x2 != null
        && x2.asInstanceOf[Boolean])
        true
      else
        null
    }
  }, None)(boolHr, boolHr, boxedboolHr)

  registerSpecial("&&", { (f1: () => Any, f2: () => Any) =>
    val x = f1()
    if (x != null) {
      if (x.asInstanceOf[Boolean])
        f2()
      else
        false
    } else {
      val x2 = f2()
      if (x2 != null
        && !x2.asInstanceOf[Boolean])
        false
      else
        null
    }
  }, None)(boolHr, boolHr, boxedboolHr)

  registerMethodSpecial("orElse", { (f1: () => Any, f2: () => Any) =>
    val v = f1()
    if (v == null)
      f2()
    else
      v
  }, None)(TTHr, TTHr, TTHr)

  registerMethod("[]", (a: IndexedSeq[Any], i: Int) => if (i >= 0) a(i) else a(a.length + i), None)(arrayHr(TTHr), intHr, TTHr)
  registerMethod("[]", (a: Map[String, Any], i: String) => a(i), None)(dictHr(TTHr), stringHr, TTHr)
  registerMethod("[]", (a: String, i: Int) => (if (i >= 0) a(i) else a(a.length + i)).toString, None)(stringHr, intHr, charHr)

  registerMethod("[:]", (a: IndexedSeq[Any]) => a, None)(arrayHr(TTHr), arrayHr(TTHr))
  registerMethod("[*:]", (a: IndexedSeq[Any], i: Int) => a.slice(i, a.length), None)(arrayHr(TTHr), intHr, arrayHr(TTHr))
  registerMethod("[:*]", (a: IndexedSeq[Any], i: Int) => a.slice(0, i), None)(arrayHr(TTHr), intHr, arrayHr(TTHr))
  registerMethod("[*:*]", (a: IndexedSeq[Any], i: Int, j: Int) => a.slice(i, j), None)(arrayHr(TTHr), intHr, intHr, arrayHr(TTHr))
}
