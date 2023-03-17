package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.metanome.algorithms.many.bitvectors.{BitVector, LongArrayBitVector}

import java.io.File

object DiscoveryMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val version = "0.5" //TODO: update this if discovery algorithm changes!
  val sourceDirs = args(0).split(",")
    .map(new File(_))
    .toIndexedSeq
  val targetDir = new File(args(1) + s"/$version/")
  targetDir.mkdir()
  val targetFileBinary = args(2)
  val epsilon = args(3).toDouble
  val deltaInDays = args(4).toLong
  val relaxedShiftedTemporalINDDiscovery = new RelaxedShiftedTemporalINDDiscovery(sourceDirs,targetFileBinary,targetDir,epsilon,TimeUtil.nanosPerDay*deltaInDays,version)
  relaxedShiftedTemporalINDDiscovery.discover(IndexedSeq(0,1,2,3,4,5,6,7,8,9,10,15,20))
}
