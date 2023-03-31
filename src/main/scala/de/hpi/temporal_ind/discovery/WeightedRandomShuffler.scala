package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.original.SimpleCounter

import java.time.Instant
import scala.collection.mutable
import scala.util.Random

class WeightedRandomShuffler(random:Random) {

  def shuffle[T](elementToWeight: collection.Iterable[(T, Int)]) = {
    var cumSum = 0L
    val cumSumToElem = collection.mutable.TreeMap[Long,T]() ++ elementToWeight
      .map { case (k, v) =>
        val res = (cumSum, k)
        cumSum += v
        res
      }
    val sortedShuffle = collection.mutable.ArrayBuffer[T]()
    while (!cumSumToElem.isEmpty) {
      val drawn = random.nextLong(cumSum)
      val selected = if (cumSumToElem.contains(drawn))
        (drawn, cumSumToElem(drawn))
      else {
        if(cumSumToElem.maxBefore(drawn).isEmpty)
          println()
        cumSumToElem.maxBefore(drawn).get
      }
      val keyAfterSelected = cumSumToElem.minAfter(selected._1+1).map(_._1).getOrElse(cumSum)
      val weightOfSelected = keyAfterSelected - selected._1
      sortedShuffle.addOne(selected._2)
      cumSumToElem.remove(selected._1)
      //update the weights of all after this, currently this is quadratic in the number of ranges, but that is probably ok - can we do this better? - probably not easily
      val toChange = collection.mutable.HashSet[Long]() ++ cumSumToElem.rangeFrom(selected._1).keySet
      toChange.foreach(oldKey => {
        val range = cumSumToElem.remove(oldKey).get
        cumSumToElem.put(oldKey - weightOfSelected, range)
      })
      cumSum -= weightOfSelected
    }
    sortedShuffle.toIndexedSeq
  }

}
