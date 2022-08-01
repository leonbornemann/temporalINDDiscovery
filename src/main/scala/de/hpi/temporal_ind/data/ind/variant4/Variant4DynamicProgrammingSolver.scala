package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.data.AbstractOrderedColumnHistory
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.TableFormatter

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedSet, mutable}

class Variant4DynamicProgrammingSolver[T <% Ordered[T]](lhs: AbstractOrderedColumnHistory[T],
                                                        rhs: AbstractOrderedColumnHistory[T],
                                                        deltaInDays: Int,
                                                        costFunction: EpsilonCostFunction) {

  def addMaximalWindowSizeMappingsTillEnd(mapping: mutable.TreeMap[(Instant, Instant), (Instant, Instant)],
                                          beginInclusive: Instant,
                                          endExclusive: Instant) = {
    var curStart = beginInclusive
    var curEnd:Option[Instant]=None
    if(beginInclusive!=endExclusive) {
      while(curEnd.isEmpty || curEnd.get != endExclusive){
        curEnd = Some(Seq(curStart.plus(Duration.ofDays(deltaInDays)).plusNanos(1),endExclusive).min)
        if(curEnd.get.isBefore(curStart) || curEnd.get.isAfter(endExclusive)) {
          assert(false)
        }
        val curInterval = (curStart, curEnd.get)
        mapping.put(curInterval,curInterval)
        curStart = curEnd.get
      }
    } else {
      //empty range - nothing to do
    }

  }

  private def completeMappingForMappingTimepoint(mapping: mutable.TreeMap[(Instant, Instant), (Instant, Instant)],
                                                 lhsTimestampIndex: Int,
                                                 rhsBegin:Instant,
                                                 rhsEndExclusive:Instant) = {
    val lhsBegin = axis(lhsTimestampIndex)._1
//    if(lhsTimestampIndex==axis.size-1) {
//      val lhsEndExclusive = GLOBAL_CONFIG.latestInstantWikipedia
//      addMaximalWindowSizeMappingsTillEnd(mapping,axis(lhsTimestampIndex)._1,lhsEndExclusive)
//    } else {
      //rest of the timestamps are mapped to their same versions:
      //to speed things up we make maximal windows until the end of time:
    val toCompleteToExclusive = if(lhsTimestampIndex==axis.size-1) GLOBAL_CONFIG.lastInstant else axis(lhsTimestampIndex+1)._1
    //the following code line is a little bit of a hack that increases the duration of the delta window from x to x + (1Day-1nanosecond)
    //this fixes an issue with the mapping that occurs because we have our timestamps at certain granularity, for example daily
    // A timestamp t in the LHS could otherwise only exactly be mapped to t-delta and the following time period until t+1 could not be mapped to that
    // (even though that is legal, because the version from the RHS also does not change from t-delta until t-delta+1)
    val maxMappedWindowEndExclusive = rhsBegin.plus(Duration.ofDays(deltaInDays)).plus(Duration.ofDays(1))
    val rhsInterval = (rhsBegin,rhsEndExclusive)
    if(maxMappedWindowEndExclusive.isAfter(toCompleteToExclusive)){
      val lhsInterval = (lhsBegin, toCompleteToExclusive)
      mapping.put(lhsInterval,rhsInterval)
    } else {
      val lhsInterval = (lhsBegin, maxMappedWindowEndExclusive)
      mapping.put(lhsInterval,rhsInterval)
      addMaximalWindowSizeMappingsTillEnd(mapping,maxMappedWindowEndExclusive,toCompleteToExclusive)
    }
//    }
  }

  def getOptimalMappingFunction = {
    var curRowIndex = axis.size-1
    val mapping = collection.mutable.TreeMap[(Instant,Instant),(Instant,Instant)]()
    (axis.size-1 to 0 by -1).foreach(lhsTimepointIndex => {
      val rhsEnd = axis(curRowIndex)._1
      val rhsStartIndex = predecessorMatrix(curRowIndex)(lhsTimepointIndex) //TODO: this is more tricky if we implement the same version may cross alternative!
      var rhsStart:Instant = Instant.MIN
      if(rhsStartIndex== -1 ){
        assert(lhsTimepointIndex==0)
        rhsStart = axis(0)._1
      } else {
        rhsStart = axis(rhsStartIndex)._1
      }
      if(ChronoUnit.DAYS.between(rhsStart,rhsEnd)>deltaInDays){
        rhsStart = rhsEnd.minus(Duration.ofDays(deltaInDays))
      }
      val mappedPeriod = (rhsStart,rhsEnd.plusNanos(1))
      //complete the mapping
      completeMappingForMappingTimepoint(mapping,lhsTimepointIndex,mappedPeriod._1,mappedPeriod._2)
      curRowIndex=rhsStartIndex
    })
    TimestampMappingFunction(mapping.toMap)
  }

  def bestMappingCost = matrix(axis.size-1)(axis.size-1)

  def isInDatasetBounds(t: Instant): Boolean = !t.isBefore(GLOBAL_CONFIG.earliestInstant) && !t.isAfter(GLOBAL_CONFIG.lastInstant)

  def deltaExtension(keySet: SortedSet[Instant]) = {
    keySet
      .flatMap(t => Set(t.minus(Duration.ofDays(deltaInDays)),t,t.plus(Duration.ofDays(deltaInDays+1))))
      .filter(t => isInDatasetBounds(t))
  }

  val axis = (lhs.history.versions.keySet ++ deltaExtension(rhs.history.versions.keySet))
    .toIndexedSeq
    .sorted
    .zipWithIndex
  val matrix = collection.mutable.ArrayBuffer[ArrayBuffer[Int]]()
  val predecessorMatrix = collection.mutable.ArrayBuffer[ArrayBuffer[Int]]()

  private val NON_EXISTANT: Int = Integer.MAX_VALUE - 1000000

  (0 until axis.size).foreach(_ => {
    val newBuffer =collection.mutable.ArrayBuffer[Int]()
    val newPredecessor = collection.mutable.ArrayBuffer[Int]()
    (0 until axis.size).foreach(_ => {
      newBuffer.append(NON_EXISTANT)
      newPredecessor.append(-1)
    })
    predecessorMatrix.append(newPredecessor)
    matrix.append(newBuffer)
  })
  assert(matrix.size == axis.size && matrix(0).size==axis.size)
  assert(matrix.size == predecessorMatrix.size && matrix(0).size == predecessorMatrix(0).size)
  //intitialize:
  initialize()
  fillMatrix()
  assert(bestMappingCost >=0)

  private def printMatrix() = {
    TableFormatter.printTable(axis.map(_._1), matrix.toSeq.map(_.toSeq))
  }

  private def printPredecessorMatrix() = {
    TableFormatter.printTable(axis.map(_._1), predecessorMatrix.toSeq.map(_.toSeq))
  }

  def getDeltaIndexWindow(index: Int) = {
    var lowerExclusiveIndex = index
    val lowerInclusiveTimestamp = axis(lowerExclusiveIndex)._1.minus(Duration.ofDays(deltaInDays))
    while(lowerExclusiveIndex >= 0 && axis(lowerExclusiveIndex)._1.isAfter(lowerInclusiveTimestamp)){
      lowerExclusiveIndex -=1
    }
    //for the lower bound, we are allowed to look one version further into the past, because that version already existed within the time bounds
    var lower = if(lowerExclusiveIndex== -1) 0 else lowerExclusiveIndex
    var upperExclusiveIndex = index
    val upperExclusiveTimestamp = axis(index)._1.plus(Duration.ofDays(deltaInDays)).plusNanos(1)
    while(upperExclusiveIndex < axis.size && axis(upperExclusiveIndex)._1.isBefore(upperExclusiveTimestamp)){
      upperExclusiveIndex +=1
    }
    val upper = upperExclusiveIndex-1
    (lower,upper)
  }

  def fillMatrix() = {
    axis.tail.foreach{case (t,j) => {
      val (lowerInclusive,upperInclusive) = getDeltaIndexWindow(j)
      (lowerInclusive to upperInclusive).foreach(i => {
        printMatrix()
        val lowerInclusive = getDeltaIndexWindow(j)._1
        val upperInclusivePredecessor = if(matrix(i)(j-1)==NON_EXISTANT) i-1 else i
        val potentialPredecessors = lowerInclusive to upperInclusivePredecessor
        val costs = potentialPredecessors
          .map{case (iPrime) =>
            val predecessorCost = matrix(iPrime)(j - 1)
            if(j==5 && iPrime==4)
              println()
            val costOfThisMapping = costFunction.cost(lhs, rhs, axis(j)._1, axis(iPrime)._1, axis(i)._1)
            val minCost = predecessorCost + costOfThisMapping
            (minCost,iPrime)
          }
        val (minimumCost,bestPredecessorIndex) = costs.min
        if(bestPredecessorIndex >= potentialPredecessors.size)
          println()
        predecessorMatrix(i)(j) = bestPredecessorIndex
        matrix(i)(j) = minimumCost
        //TODO: track mapping that is taken here!
      })
    }}
     /* TODO:  current mapping will technically be out of delta bounds, but only because we do not correct the timestamps
                  example: [1,2,3,4,5], delta=1 if there is a new version for both lhs and rhs at 1 and at 5, then 5 can map back to version 1, because version one was still active at timestamp 4
                  currently we keep this as the mapping 5-->[1,5] without correcting this to [4,5] (which is equivalent in terms of contained values)
    */
  }

  def initialize() = {
    val (t1,j) = axis.head
    val upperExclusive = t1.plus(Duration.ofDays(deltaInDays)).plusNanos(1)
    axis
      .takeWhile{case (t,i) => t.isBefore(upperExclusive)}
      .foreach{case (t,i) => {
        val tLHS = axis(j)._1
        val lowerBoundRHS = axis(0)._1
        val upperBoundRHS = axis(i)._1
        matrix(i)(j) = costFunction.cost(lhs,rhs,tLHS,lowerBoundRHS,upperBoundRHS)
      }}
  }
  //initialize:

}
