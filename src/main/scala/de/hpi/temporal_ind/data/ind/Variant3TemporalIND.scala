package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.OrderedColumnHistory

import java.time.Duration

class Variant3TemporalIND(lhs: OrderedColumnHistory, rhs: OrderedColumnHistory, deltaInDays: Int)  extends TemporalIND(lhs,rhs) {

  override def toString: String =  s"Variant3TemporalIND(${lhs.id},${rhs.id},$deltaInDays)"

  override def isValid: Boolean = {
    //create mapping function:
    var lastUsedTimestamp = rhs.history.versions.firstKey.minusNanos(1)
    val timestampIterator = allRelevantDeltaTimestamps(deltaInDays).iterator
    var allValid = !rhs.history.versions.firstKey.minus(Duration.ofDays(deltaInDays)).isAfter(lhs.history.versions.firstKey) //change this logic if we allow empty version matches
    while(timestampIterator.hasNext && allValid){
      val t = timestampIterator.next()
      var valuesToCover = collection.mutable.HashSet[String]() ++ lhs.versionAt(t).values
      val rangeStart = Seq(lastUsedTimestamp,t.minus(Duration.ofDays(deltaInDays))).max
      val rangeEnd = t.plus(Duration.ofDays(deltaInDays))
      assert(rangeEnd.isAfter(rangeStart))
      val range = rhs.history.versions.rangeImpl(Some(rangeStart),Some(rangeEnd.plusNanos(1))).iterator
      if(!rhs.history.versions.contains(rangeStart)){
        //we are allowed to use the previous version if it was not used before!
        valuesToCover = valuesToCover.diff(rhs.versionAt(rangeStart).values)
        lastUsedTimestamp = rangeStart
      }
      while(range.hasNext && !valuesToCover.isEmpty){
        val (tRHS,rhsVersion) = range.next()
        lastUsedTimestamp = tRHS
        valuesToCover = valuesToCover.diff(rhsVersion.values)
      }
      if(!valuesToCover.isEmpty){
        allValid=false
      }
    }
    allValid
  }
}
