package de.hpi.temporal_ind.discovery

import java.io.PrintWriter
import scala.io.Source
import scala.util.Random

object OneshotMain extends App {
  val weights = IndexedSeq(("B",3),
    ("A",5),
    ("C",2))
  //draw 10000 samples:
  val shuffler = new WeightedRandomShuffler(new Random(13))
  val permutationToCounts = (0 until 100000)
    .map(_ => shuffler.shuffle(weights))
    .groupBy(identity)
    .map(t => (t._1,t._2.size / 100000.0))
  permutationToCounts.foreach(println)
  val permutations = Seq(
    (("A,B,C").split(",").toIndexedSeq,(5/10.0) * (3/5.0)),
    (("A,C,B").split(",").toIndexedSeq,(5/10.0) * (2/5.0)),
    (("B,A,C").split(",").toIndexedSeq,(3/10.0) * (5/7.0)),
    (("B,C,A").split(",").toIndexedSeq,(3/10.0) * (2/7.0)),
    (("C,A,B").split(",").toIndexedSeq,(2/10.0) * (5/8.0)),
    (("C,B,A").split(",").toIndexedSeq,(2/10.0) * (3/8.0))
  )
  permutations.foreach(p => {
    println(p._1,permutationToCounts(p._1),p._2,permutationToCounts(p._1)-p._2)
  })
//  //A,
//  println(IndexedSeq("A,B,C"),(5/10.0) * (3/5.0))
//  println("A,C,B",(5/10.0) * (2/5.0))
//  //B,
//  println("B,A,C",(3/10.0) * (5/7.0))
//  println("B,C,A",(3/10.0) * (2/7.0))
//  //C,
//  println("C,A,B",(2/10.0) * (5/8.0))
//  println("C,B,A",(2/10.0) * (3/8.0))

}
