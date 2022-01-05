package de.hpi.temporal_ind.data.column.data

import java.time.{Duration, Instant}

class TimeIntervalSequence(var intervals: IndexedSeq[(Instant, Instant)]) {

  intervals = intervals.sorted

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
      val (s,e) = iterator.next()
      if(!curInterval._2.isAfter(s)){
        consolidated += curInterval
        curInterval = (s,e)
      } else {
        curInterval = (curInterval._1,e)
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
      val (s,e) = iterator.next()
      if(curInterval._2.isAfter(s)){
        consolidated.append((s,curInterval._2))
      }
      curInterval = (s,e)
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


  def summedDuration = if(intervals.size==0) Duration.ZERO
  else if(intervals.size==1)
    Duration.between(intervals(0)._1,intervals(0)._2)
  else
    intervals.map(t => Duration.between(t._1,t._2)).reduce(_.plus(_))

  def unionOfDiffs(other: TimeIntervalSequence) = {
    val diff1 = this.diff(other)
    val diff2 = other.diff(this)
    diff1.union(diff2)
  }
}
