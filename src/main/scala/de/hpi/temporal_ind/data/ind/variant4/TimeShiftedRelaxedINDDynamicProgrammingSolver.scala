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
                                                                     deltaInNanos: Long,
                                                                     useWildcardLogic:Boolean) {

  assert(!useWildcardLogic) //implementation does not yet make sense, because the algorithm can leave out versions


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

  def printMatrix() = {
    TableFormatter.printTable(Seq("") ++ axisLHS, matrix
      .zip(axisRHS)
      .toSeq.map(t => Seq(t._2) ++ t._1.toSeq))
  }

  def printPredecessorMatrix() = {
    TableFormatter.printTable(axisRHS, predecessorMatrix.toSeq.map(_.toSeq))
  }

  var matrix:Array[Array[Long]] = initLongMatrix()
  var predecessorMatrix:Array[Array[Int]] = initIntMatrix()
  val axisLHS = lhs.history.versions.keySet.toIndexedSeq
  val axisRHS = rhs.history.versions.keySet.toIndexedSeq
  //initialize:


  def endOfVersion(axis: IndexedSeq[Instant], i: Int) = {
    if(i+1 == axis.size) GLOBAL_CONFIG.lastInstant else axis(i+1)
  }

  def rhsIsWildcardOnlyInRange(lower: Instant, upper: Instant): Boolean = {
    val rhsVersions = rhs.versionsInWindow(lower,upper)
    rhsVersions.size==1 && rhs.versionAt(rhsVersions.head).columnNotPresent
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
      .filter{v => (!v.isBefore(lowerFKDeltaInclusive) || rhs.isLastVersionBefore(v,lowerFKDeltaInclusive.plusNanos(1)) ) && v.isBefore(upperFKDeltaExclusive)}
    val deltaExtensions = relevantPKVersions
      .flatMap{i => Seq(i.minusNanos(deltaInNanos),i.plusNanos(deltaInNanos))}
      .filter{v => (!v.isBefore(lowerFKDeltaInclusive) || rhs.isLastVersionBefore(v,lowerFKDeltaInclusive.plusNanos(1)) ) && v.isBefore(upperFKDeltaExclusive)}
    val allInterestingTimestamps = (Set(lhsBegin) ++ deltaExtensions)
      .toIndexedSeq
      .filter(v => v.isBefore(lhsEnd) && !v.isBefore(lhsBegin))
      .sorted
    assert(allInterestingTimestamps.forall(_.isBefore(lhsEnd)))
    var totalCost:Long = 0
    var processedTime:Long = 0
    (0 until allInterestingTimestamps.size).foreach{ i =>
      val currentTimespanOfLHSVersionStart = allInterestingTimestamps(i)
      val currentTimespanOfLHSVersionEnd = if(i+1==allInterestingTimestamps.size ) lhsEnd else allInterestingTimestamps(i+1)
      //due to construction there can be no
      val lowerDeltaWindowBorderInclusive = currentTimespanOfLHSVersionEnd.minusNanos(1).minusNanos(deltaInNanos)
      val upperDeltaWindowBorderExclusive = currentTimespanOfLHSVersionEnd.minusNanos(1).plusNanos(deltaInNanos)
      val valuesInPk = rhs.valueSetInWindow(lowerDeltaWindowBorderInclusive,upperDeltaWindowBorderExclusive.plusNanos(1),Some(relevantPKVersions))
      val timeDiff = ChronoUnit.NANOS.between(currentTimespanOfLHSVersionStart,currentTimespanOfLHSVersionEnd)
      if(lhsVersion.diff(valuesInPk).size==0 || (useWildcardLogic && rhsIsWildcardOnlyInRange(lowerDeltaWindowBorderInclusive,upperDeltaWindowBorderExclusive))){
        //no costs to add
      } else {
        //add costs:
        totalCost += timeDiff
      }
      processedTime += timeDiff
    }
    assert(processedTime == ChronoUnit.NANOS.between(lhsBegin,lhsEnd))
    totalCost
  }

  def initialize() = {
    (0 until axisRHS.size)
      .foreach( i => {
        matrix(i)(0) = costFunction(axisLHS(0),axisRHS(0),endOfVersion(axisRHS,i))
      })
  }

  initialize()

  def fillMatrix() = {
    for(j <- (1 until axisLHS.size)){
      for(i <- (0 until axisRHS.size)){
        val (pred,cost) = (0 to i)
          .map(iPrime => (iPrime,matrix(iPrime)(j-1) + costFunction(axisLHS(j),axisRHS(iPrime),endOfVersion(axisRHS,i))))
          .minBy{case (row,curCost) => (curCost,row)} // to make the mapping deterministic, we take the lowest row for now
        matrix(i)(j) = cost
        predecessorMatrix(i)(j) = pred
      }
    }
  }

  fillMatrix()

  def optimalMapping = {
    val mapping = collection.mutable.HashMap[AbstractColumnVersion[T],VersionRange[T]]()
    var curI = axisRHS.size-1
    (axisLHS.size-1 to 0 by -1)
      .foreach(j => {
        val predI = predecessorMatrix(curI)(j)
        mapping.put(lhs.history.versions(axisLHS(j)),VersionRange(rhs.history.versions(axisRHS(predI)),rhs.history.versions(axisRHS(curI))))
        curI = predI
      })
    mapping
  }

  def optimalMappingCost = {
    matrix(axisRHS.size-1)(axisLHS.size-1)
  }

}
