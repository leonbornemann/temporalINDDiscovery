package de.hpi.temporal_ind.data.wikipedia

import java.time.Instant

object GLOBAL_CONFIG {
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
