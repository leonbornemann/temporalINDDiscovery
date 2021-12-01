package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.util.Util

import java.time.Instant

object GLOBAL_CONFIG {
  val CANONICAL_NULL_VALUE: String = "⊥NULL⊥"
  (Some(),Some())
  def earliestInstantWikipedia: Instant = Instant.parse("2001-04-02T12:55:44Z")
  def latestInstantWikipedia: Instant = Instant.parse("2017-11-04T03:57:33Z")

  val NULL_VALUE_EQUIVALENTS = Set("","—","-","–","N/A","?","Unknown","- -","n/a","•","- - -",".","??","(n/a)")

}
