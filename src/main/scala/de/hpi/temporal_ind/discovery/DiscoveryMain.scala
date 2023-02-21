package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.metanome.algorithms.many.bitvectors.{BitVector, LongArrayBitVector}

import java.io.File

object DiscoveryMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val sourceDir = new File(args(0))
  val targetDir = new File(args(1))
  val epsilon = args(2).toDouble
  val deltaInDays = args(3).toLong
  val relaxedShiftedTemporalINDDiscovery = new RelaxedShiftedTemporalINDDiscovery(sourceDir,targetDir,epsilon,TimeUtil.nanosPerDay*deltaInDays)
  relaxedShiftedTemporalINDDiscovery.discover()
}
