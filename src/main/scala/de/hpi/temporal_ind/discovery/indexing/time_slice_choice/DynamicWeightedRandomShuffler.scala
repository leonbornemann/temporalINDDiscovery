package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import java.time.Instant
import scala.util.Random

class DynamicWeightedRandomShuffler[T](val initialWeights:collection.mutable.TreeMap[Instant, collection.mutable.HashSet[T]],
                                       random:Random) extends Iterator[(Long,Instant)] {
  var cumSum = 0L
  var cumSumToElem:collection.mutable.TreeMap[Long,Instant] = null
  recomputeCumSum()
  def recomputeCumSum() = {
    cumSum=0L
    cumSumToElem = collection.mutable.TreeMap[Long, Instant]() ++ initialWeights
      .map { case (k, v) =>
        val res = (cumSum, k)
        cumSum += v.size
        res
      }
    println("Recomputed cumsum, is now",cumSum)
    println("current top ten is",initialWeights.toIndexedSeq.sortBy(-_._2.size).take(10).map(t => (t._1,t._2.size)))
  }

  def hasNext:Boolean = !cumSumToElem.isEmpty

  def next() = {
    val drawn = random.nextLong(cumSum)
    val selected = if (cumSumToElem.contains(drawn))
      (drawn, cumSumToElem(drawn))
    else {
      if (cumSumToElem.maxBefore(drawn).isEmpty)
        println()
      cumSumToElem.maxBefore(drawn).get
    }
    println("Selected",selected)
    //update the weights:
    val versionsToRemove = initialWeights.remove(selected._2).get
    println("Contained",versionsToRemove.size,"versions")
    val additionalVersionsToRemove = collection.mutable.ArrayBuffer[Instant]()
    var totalRemoved = 0
    initialWeights.foreach(t => {
      val sizeBefore = t._2.size
      versionsToRemove.foreach(v => t._2.remove(v))
      totalRemoved += sizeBefore - t._2.size
      if(t._2.isEmpty){
        additionalVersionsToRemove.addOne(t._1)
      }
    })
    println("removed weights in sum of",totalRemoved)
    additionalVersionsToRemove.foreach(v => initialWeights.remove(v))
    recomputeCumSum()
    //for all remove
    selected
  }

}
