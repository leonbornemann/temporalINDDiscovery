package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters

import java.time.{Duration, Instant}

class ShifteddRelaxedCustomFunctionTemporalIND[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                                rhs: AbstractOrderedColumnHistory[T],
                                                                queryParameters:TINDParameters,
                                                                validationVariant:ValidationVariant.Value) extends TemporalIND(lhs,rhs,validationVariant){

  assert(validationVariant==ValidationVariant.FULL_TIME_PERIOD)
  val weightFunction = queryParameters.omega
  val deltaInNanos = queryParameters.absDeltaInNanos
  val absoluteEpsilon = queryParameters.absoluteEpsilon

  def rhsIsWildcardOnlyInRange(lower: Instant, upper: Instant): Boolean = {
    val rhsVersions = rhs.versionsInWindow(lower,upper)
    rhsVersions.size==1 && rhs.versionAt(rhsVersions.head).columnNotPresent
  }

//  def absoluteViolationScore = {
//    val movingTimeWindow = new MovingTimWindow(relevantValidationIntervals,lhs,rhs,weightFunction,deltaInNanos)
//    val windows = movingTimeWindow.toIndexedSeq
//    val totalViolationTime = windows
//      .map(_.cost)
//      .sum
//    totalViolationTime
//  }


  override def toString: String = s"GeneralizedTemporalIND(${lhs.id},${rhs.id},$deltaInNanos,$absoluteEpsilon)"

  def relevantValidationIntervals: Iterable[(Instant,Instant)] = {
    val validationIntervalsMap = collection.mutable.TreeMap[Instant,(Instant,Instant)]() ++ validationIntervals
      .intervals
      .map(i => (i._1,i))
    val duration = Duration.ofNanos(deltaInNanos)
    val eventTimestampList = (collection.mutable.SortedSet(GLOBAL_CONFIG.earliestInstant) ++ lhs.history.versions.keySet)
      .union(rhs.history.versions.keySet.flatMap(t => Set(t.minus(duration),t,t.plus(duration).plusNanos(1))))
      .filter(t => !t.isAfter(GLOBAL_CONFIG.lastInstant) && !t.isBefore(GLOBAL_CONFIG.earliestInstant))
      .toIndexedSeq
      .sorted
      .zipWithIndex
    val res = eventTimestampList.map{ case (t,i) => {
      val endFromList = if(i==eventTimestampList.size-1) GLOBAL_CONFIG.lastInstant else eventTimestampList(i+1)._1
      if(validationIntervalsMap.contains(t)){
        val end = Seq(validationIntervalsMap(t)._2,endFromList).min
        Some((t,end))
      } else if(validationIntervalsMap.maxBefore(t).isEmpty){
        None
      } else {
        val (tValBefore,intervalBefore) =validationIntervalsMap.maxBefore(t).get
        if(t==intervalBefore._2 || t.isAfter(intervalBefore._2))
          None
        else {
          val end = Seq(intervalBefore._2,endFromList).min
          Some((t,end))
        }
      }
    }}
      .filter(_.isDefined)
      .map(_.get)
    res
      //.map(t => getDurationInValidationIntervals(t,validationIntervalsMap))
  }

  def absoluteViolationScore = {
    val intervals = intervalsForValidationNew
    val movingTimeWindow = new MovingTimWindow(intervals, lhs, rhs, weightFunction, deltaInNanos)
    val totalViolationTime = movingTimeWindow
      .map(_.cost)
      .sum
    totalViolationTime
  }

  private def intervalsForValidationNew = {
    val timestamps = collection.mutable.HashSet[Instant]()
    timestamps += GLOBAL_CONFIG.earliestInstant
    timestamps ++= lhs.history.versions.keySet
    rhs.history.versions.foreach(v => {
      if (v._1.minusNanos(deltaInNanos).isAfter(GLOBAL_CONFIG.earliestInstant)) {
        timestamps += v._1.minusNanos(deltaInNanos)
      }
      if (v._1.plusNanos(deltaInNanos+1).isBefore(GLOBAL_CONFIG.lastInstant)) {
        timestamps += v._1.plusNanos(deltaInNanos+1)
      }
    })
    timestamps += GLOBAL_CONFIG.lastInstant
    val timestampsSorted = timestamps.toIndexedSeq.sorted
    val intervals = (0 until timestampsSorted.size - 1)
      .map(i => (timestampsSorted(i), timestampsSorted(i + 1)))
    intervals
  }

  override def isValid: Boolean = {
    //absoluteViolationScore <=absoluteEpsilonInNanos
    isValidNew
    //val (isValidNewMethod,timeNew) = TimeUtil.executionTimeInMS({isValidNew})
    //val (isValidOld,timeOld) = TimeUtil.executionTimeInMS({absoluteViolationScore <= absoluteEpsilonInNanos})

//    if(scoreNew != scoreOld){
//      val intervalsNew = intervalsForValidationNew
//      val intervalsOld = relevantValidationIntervals.toIndexedSeq.sortBy(_._1)
//      //intervalsNew.flatMap(t => Set(t._1,t._2)).toSet.diff(intervalsOld.flatMap(t => Set(t._1,t._2)).toSet).head == GLOBAL_CONFIG.earliestInstant
//      val isEqual = (0 until intervalsNew.size).map(i => intervalsNew(i)==intervalsOld(i))
//      val windowsNew = new MovingTimWindow(intervalsNew,lhs,rhs,weightFunction,deltaInNanos)
//        .toIndexedSeq
//      val windowsNewWithCumSum = windowsNew
//        .zipWithIndex
//        .map{case (tw,i) => (tw,windowsNew.slice(0,i+1).map(_.cost.toLong).sum)}
//      val windowsOld = new MovingTimWindow(intervalsOld, lhs, rhs, weightFunction, deltaInNanos)
//        .toIndexedSeq
//      val windowsOldWithCumSum = windowsOld
//        .zipWithIndex
//        .map { case (tw, i) => (tw, windowsOld.slice(0, i).map(_.cost.toLong).sum) }
//      val windowsEqual = (0 until windowsNew.size).map(i => windowsNew(i)==windowsOld(i))
//      val beginningToCumSumNew = windowsNewWithCumSum.map(t => (t._1.beginInclusive,t._2))
//        .toMap
//        .toIndexedSeq
//        .sortBy(_._1)
//      val beginningToCumSumOld = windowsOldWithCumSum.map(t => (t._1.beginInclusive, t._2))
//        .toMap
//        .toIndexedSeq
//        .sortBy(_._1)
//      val equalToOld = beginningToCumSumNew
//        .map(t => (t._1,beginningToCumSumOld.toMap.apply(t._1)==t._2))
//      val cumSumEqual = (0 until beginningToCumSumNew.size).map(i => beginningToCumSumNew(i)==beginningToCumSumOld(i))
//      val firstNonFind = beginningToCumSumNew.find(t => !beginningToCumSumOld.map(_._2).contains(t._2)).get
//      val indexOfViolating = beginningToCumSumNew.indexWhere(t => t._1==firstNonFind._1)-1
//      val violatingWindowNew = windowsNew(indexOfViolating)
//      val oldStart = windowsOld.indexWhere(_.beginInclusive==violatingWindowNew.beginInclusive)
//      val oldEnd = windowsOld.indexWhere(_.beginInclusive==violatingWindowNew.endExclusive)
//      val violatingWindowsOld = windowsOld.slice(oldStart,oldEnd)
//      val left = lhs.valueSetInWindow(Instant.parse("2013-11-17T14:50:13.000000001Z"),Instant.parse("2013-11-22T12:23:41.000000001Z"))
//      val right = rhs.valueSetInWindow(Instant.parse("2013-11-17T14:50:13.000000001Z").minusNanos(deltaInNanos),Instant.parse("2013-11-22T12:23:41.000000001Z").plusNanos(deltaInNanos))
//      (violatingWindowNew,windowsOld.find(_.beginInclusive==windowsNew(indexOfViolating).beginInclusive).get)
//      println()
//    }
  }

  def isValidNew:Boolean = {
    val intervals = intervalsForValidationNew
    var totalCost = 0.0
    val movingTimeWindow = new MovingTimWindow(intervals, lhs, rhs, weightFunction, deltaInNanos)
    while(movingTimeWindow.hasNext && totalCost<=absoluteEpsilon){
      totalCost += movingTimeWindow.next().cost
    }
    totalCost <=absoluteEpsilon
  }
}
