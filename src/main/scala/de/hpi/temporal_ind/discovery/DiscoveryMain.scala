package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File

object DiscoveryMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val sourceDirs = args(0).split(",")
    .map(new File(_))
    .toIndexedSeq
  val targetFileBinary = args(2)
  val epsilon = args(3).toDouble
  val deltaInDays = args(4).toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(5))
  val version = "0.92_" + timeSliceChoiceMethod //TODO: update this if discovery algorithm changes!
  val targetDir = new File(args(1) + s"/$version/")
  targetDir.mkdir()
  val subsetValidation = true
  val sampleSize=1000
  val bloomfilterSize = 4096
  val interactiveIndexBuilding = false
  val dataLoader = new InputDataManager(targetFileBinary,None)
  val relaxedShiftedTemporalINDDiscovery = new RelaxedShiftedTemporalINDDiscovery(dataLoader,
    new StandardResultSerializer(targetDir),
    epsilon,
    TimeUtil.nanosPerDay*deltaInDays,
    version,
    subsetValidation,
    bloomfilterSize,
    interactiveIndexBuilding,
    timeSliceChoiceMethod)
  relaxedShiftedTemporalINDDiscovery.discover(IndexedSeq(0,1,2,3,4,5,6,7,8,9,10),sampleSize)
}
