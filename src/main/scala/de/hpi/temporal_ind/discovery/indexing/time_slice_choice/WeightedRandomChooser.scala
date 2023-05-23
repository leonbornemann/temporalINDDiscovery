package de.hpi.temporal_ind.discovery.indexing.time_slice_choice

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.input_data.ColumnHistoryStorage

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.Random

class WeightedRandomChooser(historiesEnriched:ColumnHistoryStorage,
                               random:Random,
                               createIntervalFromStartingPoint:(Instant => (Instant,Instant))) extends Iterator[(Long,Instant)] {
  var cumSum = 0L
  var cumSumToElem:collection.mutable.TreeMap[Long,Instant] = null
  val granularity = ChronoUnit.DAYS
  var chosenIntervals = collection.mutable.ArrayBuffer[(Instant,Instant)]()

  recomputeCumSum()
  def recomputeCumSum() = {
    cumSum=0L
    val initialWeights = createTimestampsToUpdatedWeights(chosenIntervals)
    cumSumToElem = collection.mutable.TreeMap[Long, Instant]() ++ initialWeights
      .map { case (k, v) =>
        val res = (cumSum, k)
        cumSum += v.c
        res
      }
    println("Recomputed cumsum, is now",cumSum)
    println("current top ten is",initialWeights.toIndexedSeq.sortBy(-_._2.c).take(10).map(t => (t._1,t._2.c)))
  }


  case class Counter(var c: Int)

  def createTimestampsToUpdatedWeights(preChosenIntervals: collection.Iterable[(Instant, Instant)]) = {
    val timestampToCoveredVersions = collection.mutable.TreeMap[Instant, Counter]() ++ GLOBAL_CONFIG
      .ALL_DAYS
      .map(i => (i.truncatedTo(ChronoUnit.DAYS), Counter(0)))
      .filter(t => t._1.isAfter(GLOBAL_CONFIG.earliestInstant) && t._1.isBefore(GLOBAL_CONFIG.lastInstant))
    historiesEnriched.histories.foreach(h => {
      val withIndex = h.och.history.versions.toIndexedSeq
        .zipWithIndex
      withIndex.map { case (t, i) => {
        val begin = t._1.truncatedTo(ChronoUnit.DAYS)
        val end = if (i == withIndex.size - 1) GLOBAL_CONFIG.lastInstant else withIndex(i + 1)._1._1
        if (preChosenIntervals.exists { case (chosenBegin, chosenEnd) => !chosenEnd.isBefore(begin) && chosenBegin.isBefore(end) }) {
          //skip (we do nothing, because the version is already covered)
        } else {
          (0 until granularity.between(begin, end).toInt)
            .foreach(l => {
              timestampToCoveredVersions(begin.plus(l, granularity)).c += 1
            })
        }
      }
      }
    })
    timestampToCoveredVersions
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
    println("Selected",selected,"Recomputing weights")
    chosenIntervals += createIntervalFromStartingPoint(selected._2)
    recomputeCumSum()
    println("Done Recomputing weights")
    //for all remove
    selected
  }

}
