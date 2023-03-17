package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.{OrderedColumnHistory, ValidationVariant}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.{Duration, Instant}
import scala.collection.mutable

class SimpleTimeWindowTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                   rhs: AbstractOrderedColumnHistory[T],
                                                   deltaInNanos: Long,
                                                   absoluteEpsilonInNanos:Long,
                                                   useWildcardLogic:Boolean,
                                                   validationVariant:ValidationVariant.Value) extends TemporalIND(lhs,rhs,validationVariant){

  def rhsIsWildcardOnlyInRange(lower: Instant, upper: Instant): Boolean = {
    val rhsVersions = rhs.versionsInWindow(lower,upper)
    rhsVersions.size==1 && rhs.versionAt(rhsVersions.head).columnNotPresent
  }

  def absoluteViolationScore = {
    val violationTime = relevantTimestamps
      .map { case (t, dur) =>
        val lhsVersion = lhs.versionAt(t).values
        val lower = t.minus(Duration.ofNanos(deltaInNanos))
        val upper = t.plus(Duration.ofNanos(deltaInNanos).plusNanos(1))
        val values = rhs.versionTimestampsInWindow(lower, upper)
        val allContained = lhsVersion.subsetOf(values)// lhsVersion.forall(v => values.contains(v))
        if (allContained || (useWildcardLogic && rhsIsWildcardOnlyInRange(lower, upper)))
          0
        else
          dur
      }
      .sum
    violationTime
  }

  def debugViolationTimes = {
    relevantTimestamps
      .map { case (t, dur) =>
        val lhsVersion = lhs.versionAt(t).values
        val lower = t.minus(Duration.ofNanos(deltaInNanos))
        val upper = t.plus(Duration.ofNanos(deltaInNanos).plusNanos(1))
        val values = rhs.versionTimestampsInWindow(lower, upper)
        val allContained = lhsVersion.subsetOf(values) // lhsVersion.forall(v => values.contains(v))
        if (allContained || (useWildcardLogic && rhsIsWildcardOnlyInRange(lower, upper)))
          (t,0)
        else
          (t,dur)
      }.toIndexedSeq
  }

  override def toString: String = s"Variant1TemporalIND(${lhs.id},${rhs.id},$deltaInNanos)"

  def relevantTimestamps: Iterable[(Instant,Long)] = {
    val validationIntervalsMap = collection.mutable.TreeMap[Instant,(Instant,Instant)]() ++ validationIntervals
      .intervals
      .map(i => (i._1,i))
    val duration = Duration.ofNanos(deltaInNanos)
    val eventTimestampList = lhs.history.versions.keySet
      .union(rhs.history.versions.keySet.flatMap(t => Set(t.minus(duration),t,t.plus(duration).plusNanos(1))))
      .filter(t => !t.isAfter(GLOBAL_CONFIG.lastInstant) && !t.isBefore(GLOBAL_CONFIG.earliestInstant))
      .toIndexedSeq
      .zipWithIndex
    val res = eventTimestampList.map{ case (t,i) => {
      val endFromList = if(i==eventTimestampList.size-1) GLOBAL_CONFIG.lastInstant else eventTimestampList(i+1)._1
      if(validationIntervalsMap.contains(t)){
        val end = Seq(validationIntervalsMap(t)._2,endFromList).min
        Some((t,TimeUtil.durationNanos(t,end)))
      } else if(validationIntervalsMap.maxBefore(t).isEmpty){
        None
      } else {
        val (tValBefore,intervalBefore) =validationIntervalsMap.maxBefore(t).get
        if(t==intervalBefore._2 || t.isAfter(intervalBefore._2))
          None
        else {
          if(!(t.isAfter(intervalBefore._1) && t.isBefore(intervalBefore._2)))
            println()
          assert(t.isAfter(intervalBefore._1) && t.isBefore(intervalBefore._2))
          val end = Seq(intervalBefore._2,endFromList).min
          Some((t,TimeUtil.durationNanos(t,end)))
        }
      }
    }}
      .filter(_.isDefined)
      .map(_.get)
    res
      //.map(t => getDurationInValidationIntervals(t,validationIntervalsMap))
  }

  override def isValid: Boolean = {
    absoluteViolationScore <=absoluteEpsilonInNanos
  }
}
