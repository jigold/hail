package is.hail.expr.ir.lowering

import is.hail.expr.Nat
import is.hail.expr.ir._
import is.hail.expr.ir.functions.GetElement
import is.hail.types.{BlockMatrixSparsity, TypeWithRequiredness}
import is.hail.types.virtual._
import is.hail.utils.{FastIndexedSeq, FastSeq}

object BlockMatrixStage {
  def empty(eltType: Type): BlockMatrixStage =
    EmptyBlockMatrixStage(eltType)
}

case class EmptyBlockMatrixStage(eltType: Type) extends BlockMatrixStage(Array(), TInt32) {
  def blockContext(idx: (Int, Int)): IR =
    throw new LowererUnsupportedOperation("empty stage has no block contexts!")

  def blockBody(ctxRef: Ref): IR = NA(TNDArray(eltType, Nat(2)))


  override def collectBlocks(bindings: Seq[(String, Type)])(f: (IR, IR) => IR, blocksToCollect: Array[(Int, Int)]): IR = {
    assert(blocksToCollect.isEmpty)
    MakeArray(FastSeq(), TArray(f(Ref("x", ctxType), blockBody(Ref("x", ctxType))).typ))
  }
}

abstract class BlockMatrixStage(val globalVals: Array[(String, IR)], val ctxType: Type) {
  def blockContext(idx: (Int, Int)): IR

  def blockBody(ctxRef: Ref): IR

  def collectBlocks(bindings: Seq[(String, Type)])(f: (IR, IR) => IR, blocksToCollect: Array[(Int, Int)]): IR = {
    val ctxRef = Ref(genUID(), ctxType)
    val body = f(ctxRef, blockBody(ctxRef))
    val ctxs = MakeStream(blocksToCollect.map(idx => blockContext(idx)), TStream(ctxRef.typ))
    val bodyFreeVars = FreeVariables(body, supportsAgg = false, supportsScan = false)
    val bcFields = globalVals.filter { case (f, _) => bodyFreeVars.eval.lookupOption(f).isDefined }
    val bcVals = MakeStruct(bcFields.map { case (f, v) => f -> Ref(f, v.typ) } ++ bindings.map { case (name, t) => (name, Ref(name, t))})
    val bcRef = Ref(genUID(), bcVals.typ)
    val wrappedBody = bcFields.foldLeft(body) { case (accum, (f, _)) =>
      Let(f, GetField(bcRef, f), accum)
    }
    val collect = CollectDistributedArray(ctxs, bcVals, ctxRef.name, bcRef.name, wrappedBody)
    globalVals.foldRight[IR](collect) { case ((f, v), accum) => Let(f, v, accum) }
  }

  def addGlobals(newGlobals: (String, IR)*): BlockMatrixStage = {
    val outer = this
    new BlockMatrixStage(globalVals ++ newGlobals, ctxType) {
      def blockContext(idx: (Int, Int)): IR = outer.blockContext(idx)
      def blockBody(ctxRef: Ref): IR = outer.blockBody(ctxRef)
    }
  }

  def addContext(newTyp: Type)(newCtx: ((Int, Int)) => IR): BlockMatrixStage = {
    val outer = this
    val newCtxType = TStruct("old" -> ctxType, "new" -> newTyp)
    new BlockMatrixStage(globalVals, newCtxType) {
      def blockContext(idx: (Int, Int)): IR =
        makestruct("old" -> outer.blockContext(idx), "new" -> newCtx(idx))

      def blockBody(ctxRef: Ref): IR = bindIR(GetField(ctxRef, "old"))(outer.blockBody)
    }
  }
  def mapBody(f: (IR, IR) => IR): BlockMatrixStage = {
    val outer = this
    new BlockMatrixStage(globalVals, outer.ctxType) {
      def blockContext(idx: (Int, Int)): IR = outer.blockContext(idx)

      def blockBody(ctxRef: Ref): IR = f(ctxRef, outer.blockBody(ctxRef))
    }
  }
}

object LowerBlockMatrixIR {
  def apply(node: IR, typesToLower: DArrayLowering.Type, ctx: ExecuteContext, r: RequirednessAnalysis, relationalLetsAbove: Seq[(String, Type)]): IR = {

    def unimplemented[T](node: BaseIR): T =
      throw new LowererUnsupportedOperation(s"unimplemented: \n${ Pretty(node) }")

    def lowerIR(node: IR): IR = LowerToCDA.lower(node, typesToLower, ctx, r, relationalLetsAbove: Seq[(String, Type)])

    def lower(bmir: BlockMatrixIR): BlockMatrixStage = {
      if (!DArrayLowering.lowerBM(typesToLower))
        throw new LowererUnsupportedOperation("found BlockMatrixIR in lowering; lowering only TableIRs.")
      if (bmir.typ.nDefinedBlocks == 0)
        BlockMatrixStage.empty(bmir.typ.elementType)
      else lowerNonEmpty(bmir)
    }

    def lowerNonEmpty(bmir: BlockMatrixIR): BlockMatrixStage = bmir match {
      case BlockMatrixRead(reader) => reader.lower(ctx)
      case x@BlockMatrixRandom(seed, gaussian, shape, blockSize) =>
        val generator = invokeSeeded(if (gaussian) "rand_norm" else "rand_unif", seed, TFloat64, F64(0.0), F64(1.0))
        new BlockMatrixStage(Array(), TTuple(TInt64, TInt64)) {
          def blockContext(idx: (Int, Int)): IR = {
            val (i, j) = x.typ.blockShape(idx._1, idx._2)
            MakeTuple.ordered(FastSeq(i, j))
          }
          def blockBody(ctxRef: Ref): IR = {
            val len = (GetTupleElement(ctxRef, 0) * GetTupleElement(ctxRef, 1)).toI
            MakeNDArray(ToArray(mapIR(rangeIR(len))(_ => generator)), ctxRef, True())
          }
        }
      case x: BlockMatrixLiteral => unimplemented(bmir)
      case BlockMatrixMap(child, eltName, f, _) =>
        lower(child).mapBody { (_, body) =>
          NDArrayMap(body, eltName, f)
        }
      case BlockMatrixMap2(left, right, lname, rname, f, sparsityStrategy) =>
        if (left.typ.blockSize != right.typ.blockSize)
          throw new LowererUnsupportedOperation(s"Can't lower BlockMatrixMap2 with mismatched block sizes: ${ left.typ.blockSize } vs ${ right.typ.blockSize }")

        val loweredLeft = lower(left)
        val loweredRight = lower(right)
        loweredLeft
          .addGlobals(loweredRight.globalVals: _*)
          .addContext(loweredRight.ctxType)(loweredRight.blockContext).mapBody { (ctx, leftBody) =>
          NDArrayMap2(leftBody, bindIR(GetField(ctx, "new"))(loweredRight.blockBody), lname, rname, f)
        }

      case BlockMatrixBroadcast(child, inIndexExpr, shape, blockSize) => unimplemented(bmir)
      case BlockMatrixAgg(child, outIndexExpr) => unimplemented(bmir)
      case BlockMatrixFilter(child, keep) => unimplemented(bmir)
      case BlockMatrixSlice(child, slices) => unimplemented(bmir)
      case BlockMatrixDensify(child) => unimplemented(bmir)
      case BlockMatrixSparsify(child, sparsifier) => unimplemented(bmir)
      case RelationalLetBlockMatrix(name, value, body) => unimplemented(bmir)
      case ValueToBlockMatrix(child, shape, blockSize) if !child.typ.isInstanceOf[TArray] =>
        throw new LowererUnsupportedOperation("use explicit broadcast for scalars!")
      case x@ValueToBlockMatrix(child, _, blockSize) => // row major or scalar
        val nd = MakeNDArray(child, MakeTuple.ordered(FastSeq(I64(x.typ.nRows), I64(x.typ.nCols))), True())
        val v = Ref(genUID(), nd.typ)
        new BlockMatrixStage(
          Array(v.name -> nd),
          nd.typ) {
          def blockContext(idx: (Int, Int)): IR = {
            val (r, c) = idx
            NDArraySlice(v, MakeTuple.ordered(FastSeq(
              MakeTuple.ordered(FastSeq(I64(r.toLong * blockSize), I64(java.lang.Math.min((r.toLong + 1) * blockSize, x.typ.nRows)), I64(1))),
              MakeTuple.ordered(FastSeq(I64(c.toLong * blockSize), I64(java.lang.Math.min((c.toLong + 1) * blockSize, x.typ.nCols)), I64(1))))))
          }

          def blockBody(ctxRef: Ref): IR = ctxRef
        }
      case x@BlockMatrixDot(leftIR, rightIR) =>
        val left = lower(leftIR)
        val right = lower(rightIR)
        val newCtxType = TArray(TTuple(left.ctxType, right.ctxType))
        new BlockMatrixStage(left.globalVals ++ right.globalVals, newCtxType) {
          def blockContext(idx: (Int, Int)): IR = {
            val (i, j) = idx
            MakeArray(Array.tabulate[Option[IR]](leftIR.typ.nColBlocks) { k =>
              if (leftIR.typ.hasBlock(i -> k) && rightIR.typ.hasBlock(k -> j))
                Some(MakeTuple.ordered(FastSeq(
                  left.blockContext(i -> k), right.blockContext(k -> j))))
              else None
            }.flatten[IR], newCtxType)
          }

          def blockBody(ctxRef: Ref): IR = {
            def blockMultiply(elt: Ref) =
              bindIR(GetTupleElement(elt, 0)) { leftElt =>
                bindIR(GetTupleElement(elt, 1)) { rightElt =>
                  NDArrayMatMul(left.blockBody(leftElt), right.blockBody(rightElt))
                }
              }
            foldIR(ToStream(invoke("sliceRight", ctxType, ctxRef, I32(1))),
              bindIR(ArrayRef(ctxRef, 0))(blockMultiply)) { (sum, elt) =>
              NDArrayMap2(sum, blockMultiply(elt), "l", "r",
                Ref("l", x.typ.elementType) + Ref("r", x.typ.elementType))
            }
          }
        }
    }

    node match {
      case BlockMatrixCollect(child) =>
        val bm = lower(child)
        val blocksRowMajor = Array.range(0, child.typ.nRowBlocks).flatMap { i =>
          Array.tabulate(child.typ.nColBlocks)(j => i -> j).filter(child.typ.hasBlock)
        }
        val cda = bm.collectBlocks(relationalLetsAbove)((_, b) => b, blocksRowMajor)
        val blockResults = Ref(genUID(), cda.typ)

        val rows = if (child.typ.isSparse) {
          val blockMap = blocksRowMajor.zipWithIndex.toMap
          MakeArray(Array.tabulate[IR](child.typ.nRowBlocks) { i =>
            NDArrayConcat(MakeArray(Array.tabulate[IR](child.typ.nColBlocks) { j =>
              if (blockMap.contains(i -> j))
                ArrayRef(blockResults, i * child.typ.nColBlocks + j)
              else {
                val (nRows, nCols) = child.typ.blockShape(i, j)
                MakeNDArray.fill(zero(child.typ.elementType), FastIndexedSeq(nRows, nCols), True())
              }
            }, coerce[TArray](cda.typ)), 1)
          }, coerce[TArray](cda.typ))
        } else {
          val i = Ref(genUID(), TInt32)
          val j = Ref(genUID(), TInt32)
          val cols = ToArray(StreamMap(StreamRange(0, child.typ.nColBlocks, 1), j.name, ArrayRef(blockResults, i * child.typ.nColBlocks + j)))
          ToArray(StreamMap(StreamRange(0, child.typ.nRowBlocks, 1), i.name, NDArrayConcat(cols, 1)))
        }
        Let(blockResults.name, cda, NDArrayConcat(rows, 0))
      case BlockMatrixToValueApply(child, GetElement(IndexedSeq(i, j))) =>
        val rowBlock = child.typ.getBlockIdx(i)
        val colBlock = child.typ.getBlockIdx(j)

        val iInBlock = i - rowBlock * child.typ.blockSize
        val jInBlock = j - colBlock * child.typ.blockSize

        val lowered = lower(child)

        val elt = bindIR(lowered.blockContext(rowBlock -> colBlock)) { ctx =>
          NDArrayRef(lowered.blockBody(ctx), FastIndexedSeq(I64(iInBlock), I64(jInBlock)))
        }

        lowered.globalVals.foldRight[IR](elt) { case ((f, v), accum) => Let(f, v, accum) }
      case BlockMatrixWrite(child, writer) =>
        writer.lower(ctx, lower(child), child, relationalLetsAbove, TypeWithRequiredness(child.typ.elementType)) //FIXME: BlockMatrixIR is currently ignored in Requiredness inference since all eltTypes are +TFloat64
      case BlockMatrixMultiWrite(blockMatrices, writer) => unimplemented(node)
      case node if node.children.exists(_.isInstanceOf[BlockMatrixIR]) =>
        throw new LowererUnsupportedOperation(s"IR nodes with BlockMatrixIR children need explicit rules: \n${ Pretty(node) }")

      case node =>
        throw new LowererUnsupportedOperation(s"Value IRs with no BlockMatrixIR children must be lowered through LowerIR: \n${ Pretty(node) }")
    }
  }
}
