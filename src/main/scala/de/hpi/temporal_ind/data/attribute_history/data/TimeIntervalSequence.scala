package de.hpi.temporal_ind.data.attribute_history.data

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

class TimeIntervalSequence(var intervals: IndexedSeq[(Instant, Instant)],consolidate:Boolean = false) {

  intervals = intervals.sorted
  if(consolidate){
    intervals = this.union(new TimeIntervalSequence(IndexedSeq(),false)).intervals
  }

  def isDisjoint(intervals: IndexedSeq[(Instant, Instant)]): Boolean = {
    intervals.sorted.zipWithIndex.forall{case ((s,e),i) => i == intervals.size-1 || !e.isAfter(intervals(i+1)._1)}
  }

  assert(isDisjoint(intervals))

  def union(other:TimeIntervalSequence) = {
    val intervalsNew = (intervals ++ other.intervals).sorted
    val consolidated = collection.mutable.ArrayBuffer[(Instant,Instant)]()
    var curInterval = intervalsNew.head
    val iterator = intervalsNew.tail.iterator
    while(iterator.hasNext){
      val curEnd = curInterval._2
      val curStart = curInterval._1
      val (s,e) = iterator.next()
      if(curEnd.isBefore(s)){
        consolidated += curInterval
        curInterval = (s,e)
      } else {
        curInterval = (Seq(curStart,s).min,Seq(e,curEnd).max)
      }
    }
    consolidated += curInterval
    new TimeIntervalSequence(consolidated.toIndexedSeq)
  }

  def intersect(other: TimeIntervalSequence) = {
    val intervalsNew = (intervals ++ other.intervals).sorted
    val consolidated = collection.mutable.ArrayBuffer[(Instant,Instant)]()
    var curInterval = intervalsNew.head
    val iterator = intervalsNew.tail.iterator
    while(iterator.hasNext){
      val curEnd = curInterval._2
      val curStart = curInterval._1
      val (s,e) = iterator.next()
      if(curEnd.isAfter(s)){
        val maxBegin = Seq(s,curStart).max
        val minEnd = Seq(e,curEnd).min
        consolidated.append((maxBegin,minEnd))
      }
      curInterval = if(e.isAfter(curEnd)) (s,e) else curInterval
    }
    new TimeIntervalSequence(consolidated.toIndexedSeq)
  }

  def diff(other: TimeIntervalSequence) = {
    val consolidated = collection.mutable.ArrayBuffer[(Instant,Instant)]()
    val myIntervals = intervals.iterator
    val otherIntervals = other.intervals.iterator
    var curOtherInterval = otherIntervals.nextOption()
    while(myIntervals.hasNext){
      val (s,e) = myIntervals.next()
      var curStart = s
      var done = false
      while(curOtherInterval.isDefined && curOtherInterval.get._1.isBefore(e) && !done){
        //TODO: most of this logic is wrong
        val (os,oe) = curOtherInterval.get
        if(!oe.isAfter(s)){
          curOtherInterval = otherIntervals.nextOption()
        } else if(oe==e){
          consolidated.append((curStart,e))
          curOtherInterval = otherIntervals.nextOption()
          done = true
        } else if(oe.isAfter(e)){
          consolidated.append((curStart,e))
          done = true
        } else {
          assert(oe.isBefore(e))
          consolidated.append((curStart,oe))
          curOtherInterval = otherIntervals.nextOption()
        }
      }
      if(curOtherInterval.isEmpty){
        consolidated.append((curStart,e))
      }
    }
    new TimeIntervalSequence(consolidated.toIndexedSeq)
  }


  def summedDurationNanos = if(intervals.size==0) 0
    else if(intervals.size==1)
      ChronoUnit.NANOS.between(intervals(0)._1,intervals(0)._2)
    else
      intervals.map(t => ChronoUnit.NANOS.between(t._1,t._2)).reduce(_ +_)

  def unionOfDiffs(other: TimeIntervalSequence) = {
    val diff1 = this.diff(other)
    val diff2 = other.diff(this)
    diff1.union(diff2)
  }
}

object TimeIntervalSequence{

  def fromSortedStartTimes(startTimes: IndexedSeq[Instant], lastEnd: Instant): TimeIntervalSequence = {
    val withIndex = (startTimes ++ Seq(lastEnd)).zipWithIndex
    new TimeIntervalSequence(withIndex
      .withFilter(_._2 != startTimes.size)
      .map { case (s, i) => (s, withIndex(i + 1)._1) })
  }

}
