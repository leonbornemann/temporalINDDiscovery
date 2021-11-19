package de.hpi.temporal_ind.data.wikipedia

import de.hpi.temporal_ind.util.Util

import java.time.Instant

object GLOBAL_CONFIG {

  def earliestInstantWikipedia: Instant = Util.instantFromWikipediaDateTimeString("Thu Mar 21 22:19:05 CET 2002")
  def latestInstantWikipedia: Instant = Util.instantFromWikipediaDateTimeString("Thu Mar 21 22:19:05 CET 2019")

  val NULL_VALUE_EQUIVALENTS = Set("","—","-","–","N/A","?","Unknown","- -","n/a","•","- - -",".","??","(n/a)")

}
