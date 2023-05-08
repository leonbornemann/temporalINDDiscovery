package de.hpi.temporal_ind.data.ind.traversal

import de.hpi.temporal_ind.data.attribute_history.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.ind.weight_functions.TimestampWeightFunction

import java.time.Instant

class MovingTimeWindow[T](validationIntervals: Iterable[(Instant, Instant)],
                          lhs: AbstractOrderedColumnHistory[T],
                          rhs: AbstractOrderedColumnHistory[T],
                          costFunction:TimestampWeightFunction,
                          deltaInNanos: Long) extends Iterator[TimeWindowWithCost]{

  val it = validationIntervals.iterator
  val lhsIterator = new TemporallyUnionedValueSetIterator(lhs,deltaInNanos)
  val rhsIterator = new TemporallyUnionedValueSetIterator(rhs,deltaInNanos)

  var curVersionsOfRHS = collection.mutable.ArrayBuffer()

  def hasNext = it.hasNext

  override def next(): TimeWindowWithCost = {
    val (curBegin,curEnd) = it.next()
    val deltaExtendedBegin = curBegin.minusNanos(deltaInNanos)
    val deltaExtendedEnd = curBegin.plusNanos(deltaInNanos+1)
    if (curEnd == Instant.parse("1970-01-01T00:00:00.000000015Z")) {
      println()
    }
    val setLHS:collection.Set[T] = lhsIterator.advanceToInterval(curBegin,curEnd)
    val setRHS:collection.Set[T] = rhsIterator.advanceToInterval(deltaExtendedBegin,deltaExtendedEnd)
    if(setLHS.subsetOf(setRHS)){
      TimeWindowWithCost(curBegin,curEnd,0.0)
    } else {
      TimeWindowWithCost(curBegin,curEnd,costFunction.weight(curBegin,curEnd))
    }
  }

  //.map { case (t, dur) =>
  //        val lhsVersion = lhs.versionAt(t).values
  //        val lower = t.minus(Duration.ofNanos(deltaInNanos))
  //        val upper = t.plus(Duration.ofNanos(deltaInNanos).plusNanos(1))
  //        val values = rhs.valuesInWindow(lower, upper)
  //        val allContained = lhsVersion.subsetOf(values) // lhsVersion.forall(v => values.contains(v))
  //        if (allContained || (useWildcardLogic && rhsIsWildcardOnlyInRange(lower, upper)))
  //          0
  //        else
  //          dur
  //     }
}
