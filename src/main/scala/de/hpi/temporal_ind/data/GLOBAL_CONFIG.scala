package de.hpi.temporal_ind.data

import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.util.TimeUtil

import java.time.Instant
import java.time.temporal.ChronoUnit

object GLOBAL_CONFIG {
  val PARALLEL_TIND_VALIDATION_BATCH_SIZE = 2

  def ALL_DAYS = (0L to ChronoUnit.DAYS.between(earliestInstant,lastInstant)).map(l => earliestInstant.plus(l,ChronoUnit.DAYS))

  def partitionTimePeriodIntoSlices(expectedQueryParameters: TINDParameters) = {
    //assert(false) //needs rework
    println("WARNING- using old version of partitionTimePeriodIntoSlices - this still needs a rework ")
    val sliceSizeNanos = expectedQueryParameters.absoluteEpsilon.toLong
    val numSlices = Math.ceil(TimeUtil.durationNanos(earliestInstant,lastInstant)/sliceSizeNanos.toDouble).toLong
    (0L until numSlices)
      .map(i => {
        val begin = GLOBAL_CONFIG.earliestInstant.plusNanos(i * sliceSizeNanos)
        val end = Seq(GLOBAL_CONFIG.earliestInstant.plusNanos(i * (sliceSizeNanos + 1)),GLOBAL_CONFIG.lastInstant).min
        (begin, end)
      })
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
