package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.TableFormatter

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}

class TimeShiftedRelaxedINDDynamicProgrammingSolver[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                                     rhs: AbstractOrderedColumnHistory[T],
                                                                     deltaInNanos: Long) {
  def optimalMappingRelativeCost = {
    TimeUtil.toRelativeTimeAmount(optimalMappingCost)
  }


  def initLongMatrix() = {
    (0 until rhs.history.versions.keySet.size)
      .map(_ => new Array[Long](lhs.history.versions.keySet.size))
      .toArray
  }

  def initIntMatrix[T]() = {
    (0 until rhs.history.versions.keySet.size)
      .map(_ => new Array[Int](lhs.history.versions.keySet.size))
      .toArray
  }

  var matrix:Array[Array[Long]] = initLongMatrix()
  var precessorMatrix:Array[Array[Int]] = initIntMatrix()
  val axisLHS = lhs.history.versions.keySet.toIndexedSeq
  val axisRHS = rhs.history.versions.keySet.toIndexedSeq
  //initialize:


  def endOfVersion(axis: IndexedSeq[Instant], i: Int) = {
    if(i+1 == axis.size) GLOBAL_CONFIG.lastInstant else axis(i+1)
  }

  def costFunction(lhsBegin: Instant, rhsBeginInclusive: Instant, rhsEndExclusive: Instant): Long = {
    assert(rhs.history.versions.contains(rhsBeginInclusive))
    assert(rhs.history.versions.contains(rhsEndExclusive) || rhsEndExclusive==GLOBAL_CONFIG.lastInstant)
    val lhsVersion = lhs.history.versions(lhsBegin).values
    val lhsEnd = lhs.history.versions.minAfter(lhsBegin.plusNanos(1))
      .map(o => o._1)
      .getOrElse(GLOBAL_CONFIG.lastInstant)
    val lowerFKDeltaInclusive = lhsBegin.minus(deltaInNanos,ChronoUnit.NANOS)
    val upperFKDeltaExclusive = lhsEnd.plus(deltaInNanos,ChronoUnit.NANOS)
    val relevantPKVersions = rhs.versionsInWindow(rhsBeginInclusive,rhsEndExclusive)
      .filter{v => !v.isBefore(lowerFKDeltaInclusive) && v.isBefore(upperFKDeltaExclusive)}
    val deltaExtensions = relevantPKVersions
      .flatMap{i => Seq(i.minusNanos(deltaInNanos),i.plusNanos(deltaInNanos+1))}
      .filter{v => !v.isBefore(lowerFKDeltaInclusive) && v.isBefore(upperFKDeltaExclusive)}

    val allInterestingTimestamps = (Set(lhsBegin) ++ relevantPKVersions ++ deltaExtensions)
      .toIndexedSeq
      .filter(v => v.isBefore(lhsEnd) && !v.isBefore(lhsBegin))
      .sorted
    assert(allInterestingTimestamps.forall(_.isBefore(lhsEnd)))
    var totalCost:Long = 0
    var processedTime:Long = 0
    (0 until allInterestingTimestamps.size).foreach{ i =>
      val currentWindowStart = allInterestingTimestamps(i)
      val currentWindowEnd = if(i+1==allInterestingTimestamps.size ) lhsEnd else allInterestingTimestamps(i+1)
      //due to construction there can be no
      val lower = currentWindowEnd.minusNanos(1).minusNanos(deltaInNanos)
      val upper = currentWindowEnd.minusNanos(1).plusNanos(deltaInNanos)
      val valuesInPk = rhs.valuesInWindow(lower,upper.plusNanos(1),Some(relevantPKVersions))
      val timeDiff = ChronoUnit.NANOS.between(currentWindowStart,currentWindowEnd)
      if(lhsVersion.diff(valuesInPk).size!=0){
        //add costs
        totalCost += timeDiff
      } else {
        //no costs to add
      }
      processedTime += timeDiff
    }
    assert(processedTime == ChronoUnit.NANOS.between(lhsBegin,lhsEnd))
    totalCost
  }

  def initialize() = {
    (0 until axisRHS.size)
      .foreach( i => matrix(i)(0) = costFunction(axisLHS(0),axisRHS(0),endOfVersion(axisRHS,i)))
  }

  initialize()

  def fillMatrix() = {
    for(j <- (1 until axisLHS.size)){
      for(i <- (0 until axisRHS.size)){
        val (pred,cost) = (0 to i)
          .map(iPrime => (iPrime,matrix(iPrime)(j-1) + costFunction(axisLHS(j),axisRHS(iPrime),endOfVersion(axisRHS,i))))
          .minBy{case (row,curCost) => (curCost,row)} // to make the mapping deterministic, we take the lowest row for now
        matrix(i)(j) = cost
        precessorMatrix(i)(j) = pred
      }
    }
  }

  fillMatrix()

  def optimalMapping = {
    val mapping = collection.mutable.HashMap[AbstractColumnVersion[T],VersionRange[T]]()
    var curI = axisRHS.size-1
    (axisLHS.size-1 to 0 by -1)
      .foreach(j => {
        val predI = precessorMatrix(curI)(j)
        mapping.put(lhs.history.versions(axisLHS(j)),VersionRange(rhs.history.versions(axisRHS(predI)),rhs.history.versions(axisRHS(curI))))
        curI = predI
      })
    mapping
  }

  def optimalMappingCost = {
    matrix(axisRHS.size-1)(axisLHS.size-1)
  }

}
