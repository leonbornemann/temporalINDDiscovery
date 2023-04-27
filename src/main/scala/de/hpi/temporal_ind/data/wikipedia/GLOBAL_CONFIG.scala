package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil

import java.time.Instant
import java.time.temporal.ChronoUnit

object GLOBAL_CONFIG {
  var totalTimeNew = 0.0
  var totalTimeOld = 0.0

  def partitionTimePeriodIntoSlices(sliceSizeNanos: Long) = {
    val numSlices = Math.ceil(TimeUtil.durationNanos(earliestInstant,lastInstant)/sliceSizeNanos.toDouble).toLong
    (0L until numSlices)
      .map(i => {
        val begin = GLOBAL_CONFIG.earliestInstant.plusNanos(i * sliceSizeNanos)
        val end = Seq(GLOBAL_CONFIG.earliestInstant.plusNanos(i * (sliceSizeNanos + 1)),GLOBAL_CONFIG.lastInstant).min
        (begin, end)
      })
  }

  def getAllTimeSlices() = {

  }

  def totalTimeInNanos = ChronoUnit.NANOS.between(earliestInstant,lastInstant)
  def totalTimeInDays = ChronoUnit.DAYS.between(earliestInstant,lastInstant)

  val CANONICAL_NULL_VALUE: String = "⊥NULL⊥"

  var earliestInstant: Instant = null
  var lastInstant: Instant = null

  def setSettingsForDataSource(source:String) = {
    source match {
      case "wikipedia" => {
        earliestInstant = Instant.parse("2001-04-02T12:55:44Z")
        //one day plus one nanosecond after latest actual timestamp:
        lastInstant = Instant.parse("2017-11-05T03:57:33Z").plusNanos(1)

      }
      case _ => throw new AssertionError(s"Datasource $source not recognized")
    }
  }

  (Some(),Some())
  val NULL_VALUE_EQUIVALENTS = Set("","—","-","–","N/A","?","Unknown","- -","n/a","•","- - -",".","??","(n/a)")
}
