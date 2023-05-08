package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.{AbstractColumnVersion, AbstractOrderedColumnHistory}
import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnVersion
import de.hpi.temporal_ind.data.attribute_history.data.traversal.PeekableIterator

import java.time.Instant
import scala.collection.mutable.MultiSet
class TemporallyUnionedValueSetIterator[T](history: AbstractOrderedColumnHistory[T],
                                           deltaInNanos:Long){

  val versions = history.history.versions.toIndexedSeq
  var curLeftIndex = -1
  var curRightIndex = 0
  val valueSetInInterval = collection.mutable.MultiSet[T]()

  private def advanceInterval() = {
    valueSetInInterval --= leftBorder._2.values
    valueSetInInterval ++= rightBorder._2.values
    curLeftIndex+=1
    curRightIndex+=1
  }

  private def advanceLeftBorderTo(beginInclusive: Instant) = {
    assert(!leftBorder._1.isAfter(beginInclusive))
    if(beginInclusive==leftBorder._1){
      //nothing TODO here
    } else {
      assert(!rightBorder._1.isBefore(beginInclusive))
      while(versions(curLeftIndex+1)._1.isBefore(beginInclusive)){
        advanceLeftBorder()
        assert(leftBorder._1.isBefore(rightBorder._1))
      }
      if(versions(curLeftIndex+1)._1 == beginInclusive){
        advanceLeftBorder()
      }
    }
    assert(beginInclusive==leftBorder._1 || beginInclusive.isAfter(leftBorder._1) && beginInclusive.isBefore(versions(curLeftIndex+1)._1) )
  }

  private def advanceLeftBorder(): Unit = {
    valueSetInInterval --= leftBorder._2.values
    curLeftIndex += 1
  }

  private def advanceRightBorder(): Unit = {
    valueSetInInterval ++= rightBorder._2.values
    curRightIndex += 1
  }
  private def advanceRightBorderTo(endExclusive: Instant) = {
    while(rightBorder._1.isBefore(endExclusive)){
      advanceRightBorder()
    }
    assert(!endExclusive.isAfter(rightBorder._1))
  }

  def leftBorder = {
    if(curLeftIndex== -1){
      (GLOBAL_CONFIG.earliestInstant.minusNanos(deltaInNanos),AbstractColumnVersion.getEmpty[T]())
    } else {
      versions(curLeftIndex)
    }
  }

  def rightBorder = {
    if(curRightIndex==versions.size){
      (GLOBAL_CONFIG.lastInstant.plusNanos(deltaInNanos),AbstractColumnVersion.getEmpty[T]())
    } else {
      versions(curRightIndex)
    }
  }

  def advanceToInterval(beginInclusive: Instant, endExclusive: Instant): collection.Set[T] = {
    if(!beginInclusive.isBefore(versions(versions.size-1)._1)){
      //just return last version
      valueSetInInterval.clear()
      valueSetInInterval.addAll(versions(versions.size-1)._2.values)
      curLeftIndex=versions.size-1
      curRightIndex=versions.size
      versions(versions.size-1)._2.values
    } else {
      if (leftBorder._1.isAfter(beginInclusive)) {
        throw new AssertionError("Iterator is already past requested interval")
      }
      while (rightBorder._1.isBefore(beginInclusive)) {
        //advance interval
        advanceInterval()
      }
      advanceLeftBorderTo(beginInclusive)
      advanceRightBorderTo(endExclusive)
      valueSetInInterval.toSet
    }

  }


}
