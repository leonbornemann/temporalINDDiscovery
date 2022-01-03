package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.OrderedColumnHistory
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.util.TableFormatter

import java.time.Duration

abstract class TemporalIND(val lhs: OrderedColumnHistory, val rhs: OrderedColumnHistory) {

  def isValid:Boolean

  def allRelevantTimestamps = {
    //lhs
    lhs.history.versions.keySet.union(rhs.history.versions.keySet)
  }

  def allRelevantDeltaTimestamps(deltaInDays:Int) = {
    //lhs
    val duration = Duration.ofDays(deltaInDays)
    lhs.history.versions.keySet.flatMap(t => Set(t.minus(duration),t,t.plus(duration)))
      .union(rhs.history.versions.keySet.flatMap(t => Set(t.minus(duration),t,t.plus(duration))))
      .filter(t => !t.isAfter(GLOBAL_CONFIG.latestInstantWikipedia) && !t.isBefore(GLOBAL_CONFIG.earliestInstantWikipedia))
  }

  def getTabularEventLineageString = {
    val allDates = lhs.history.versions.keySet.union(rhs.history.versions.keySet).toIndexedSeq.sorted
    val cells1 = IndexedSeq(lhs.id) ++ allDates.map(t => lhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val cells2 = IndexedSeq(rhs.id) ++ allDates.map(t => rhs.versionAt(t).values.toIndexedSeq.sorted.mkString(","))
    val header = Seq(toString + s" -- isValid:$isValid") ++ allDates
    TableFormatter.format(Seq(header) ++ Seq(cells1,cells2))
  }
}
