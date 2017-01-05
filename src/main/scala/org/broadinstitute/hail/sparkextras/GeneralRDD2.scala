package org.broadinstitute.hail.sparkextras

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

//case class ParentInput[T](rdd: RDD[T], index: Int) {
//  def partition = rdd.partitions(index)
//}

//case class PartitionInput2[T](index: Int, parentInputs: Array[ParentInput[T]], f: (Array[Iterator[T]] => Iterator[T]))

case class GeneralRDDPartition2[T](index: Int, inputs: Array[(Int, Int)], f: (Array[Iterator[T]]) => Iterator[T]) extends Partition

class GeneralRDD2[T](@transient var sc: SparkContext,
  var rdds: Array[RDD[T]],
  var inputs: Array[(Array[(Int, Int)], (Array[Iterator[T]] => Iterator[T]))])(implicit tct: ClassTag[T]) extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    inputs.zipWithIndex.map { case (input, i) => new GeneralRDDPartition2[T](i, input._1, input._2)}
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val gp = split.asInstanceOf[GeneralRDDPartition2[T]]
    gp.f(gp.inputs.map { case (rddIndex, partitionIndex) =>
      val rdd = rdds(rddIndex)
      val partition = rdd.partitions(partitionIndex)
      rdd.iterator(partition, context)
    })
  }

  override def getDependencies: Seq[Dependency[_]] = {
    inputs
      .zipWithIndex
      .flatMap{case (input, i) => input._1.map{ case (rddIndex, partitionIndex) => (rddIndex, partitionIndex, i)}}
      .groupBy(_._1)
      .map{ case (rddIndex, x) =>
        new NarrowDependency[T](rdds(rddIndex)) {
          override def getParents(partitionId: Int): Seq[Int] =
            x.filter{ partitionId == _._3 }.map(_._2)
        }
      }.toSeq
  }

//  override def getDependencies: Seq[Dependency[_]] = {
//    val rdds = inputs.map {
//      _.parentInputs.map(_.rdd)
//    }
//    inputs.flatMap(_.parentInputs.map { input =>
//      println(s"parentInput=${ input } parentPartitionIndex=${ input.index }")
//      new NarrowDependency[T](input.rdd) {
//        //      println(s"getDependencies: ${input.rdd}")
//        override def getParents(partitionId: Int): Seq[Int] = {
//          println(s"getParents: partitionId: $partitionId rdd=${ this.rdd } index: ${ input.index }")
//          Seq(input.index)
//        }
//      }
//    })
//  }

  //  override def getDependencies: Seq[Dependency[_]] = {
  //    inputs.flatMap(_.parentInputs.map { input =>
  //      println(s"parentInput=${input} parentPartitionIndex=${input.index}")
  //      new NarrowDependency[T](input.rdd) {
  ////      println(s"getDependencies: ${input.rdd}")
  //      override def getParents(partitionId: Int): Seq[Int] = {
  //        println(s"getParents: partitionId: $partitionId rdd=${this.rdd} index: ${input.index}")
  //        Seq(input.index)
  //      }
  //    }})
  //  }
}