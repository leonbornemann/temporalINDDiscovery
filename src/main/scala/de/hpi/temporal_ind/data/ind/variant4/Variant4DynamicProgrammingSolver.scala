package de.hpi.temporal_ind.data.ind.variant4

import de.hpi.temporal_ind.data.column.{ColumnVersion, OrderedColumnHistory}
import de.hpi.temporal_ind.util.TableFormatter

import java.time.{Duration, Instant}
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer

class Variant4DynamicProgrammingSolver(lhs: OrderedColumnHistory, rhs: OrderedColumnHistory, deltaInDays: Int, costFunction: EpsilonCostFunction) {

  def getOptimalMappingFunction = {
    var curRowIndex = axis.size-1
    val mapping = collection.mutable.TreeMap[Instant,Duration]()
    (axis.size-1 to 0 by -1).foreach(j => {
      val curEnd = axis(curRowIndex)._1.plusNanos(1)
      val curStartIndex = predecessorMatrix(j)(curRowIndex)
      val curStart = axis(curStartIndex)
      mapping.put(axis(j)._1,Duration.between(curStart._1,curEnd))
      curRowIndex=curStartIndex
    })
    //TODO: return mapping as a class!
    TimestampMappingFunction(mapping.toMap)
  }

  def bestMappingCost = matrix(axis.size-1)(axis.size-1)

  val axis = (lhs.history.versions.keySet ++ rhs.history.versions.keySet)
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
  printMatrix

  private def printMatrix() = {
    TableFormatter.printTable(axis.map(_._1), matrix.toSeq.map(_.toSeq))
  }

  true==true

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
        val lowerInclusive = getDeltaIndexWindow(j)._1
        val upperInclusivePredecessor = if(matrix(i)(j-1)==NON_EXISTANT) i-1 else i
        val potentialPredecessors = lowerInclusive to upperInclusivePredecessor
        if(j==1 && i ==2) {
          printMatrix()
          println()
        }
        val costs = potentialPredecessors
          .map{case (iPrime) =>
            val predecessorCost = matrix(iPrime)(j - 1)
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
