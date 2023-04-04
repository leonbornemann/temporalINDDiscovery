package de.hpi.temporal_ind.discovery

import de.metanome.algorithms.many.bitvectors.{BitVector, LongArrayBitVector}

import java.io.PrintWriter
import scala.io.Source
import scala.util.Random

object OneshotMain extends App {

  val a = new LongArrayBitVector(16)
  println(a)
  a.set(1)
  a.set(3)
  println(a)
  println(a.flip())

//  val target = new PrintWriter("/home/leon/data/temporalINDDiscovery/wikipedia/discovery/fromIsfet/0.7_explore_time_slice/timeSliceStatsFixed.csv")
//  val lines = Source
//    .fromFile("/home/leon/data/temporalINDDiscovery/wikipedia/discovery/fromIsfet/0.7_explore_time_slice/timeSliceStats_bad.csv")
//    .getLines()
//    .toIndexedSeq
//  target.println(lines.head)
//  lines
//    .tail
//    .map(s => {
//      val tokens = s.split(",")
//      val tokens1 = tokens(0).split("Z\\.")
//      val newTokens = Seq(tokens1(0)+"Z",tokens1(1)) ++ tokens.tail
//      target.println(newTokens.mkString(","))
//    })
//  target.close()

//
//  val weights = IndexedSeq(("B", 3),
//    ("A", 5),
//    ("C", 2))
//  //draw 10000 samples:
//  val shuffler = new WeightedRandomShuffler(new Random(13))
//  val permutationToCounts = (0 until 100000)
//    .map(_ => shuffler.shuffle(weights))
//    .groupBy(identity)
//    .map(t => (t._1, t._2.size / 100000.0))
//  permutationToCounts.foreach(println)
//  val permutations = Seq(
//    (("A,B,C").split(",").toIndexedSeq, (5 / 10.0) * (3 / 5.0)),
//    (("A,C,B").split(",").toIndexedSeq, (5 / 10.0) * (2 / 5.0)),
//    (("B,A,C").split(",").toIndexedSeq, (3 / 10.0) * (5 / 7.0)),
//    (("B,C,A").split(",").toIndexedSeq, (3 / 10.0) * (2 / 7.0)),
//    (("C,A,B").split(",").toIndexedSeq, (2 / 10.0) * (5 / 8.0)),
//    (("C,B,A").split(",").toIndexedSeq, (2 / 10.0) * (3 / 8.0))
//  )
//  permutations.foreach(p => {
//    println(p._1, permutationToCounts(p._1), p._2, permutationToCounts(p._1) - p._2)
//  })
}
