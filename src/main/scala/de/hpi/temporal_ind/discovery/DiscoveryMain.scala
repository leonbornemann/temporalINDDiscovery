package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.ColumnHistoryID
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer

import java.io.File

object DiscoveryMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val queryFile = args(0)
  val sourceFileBinary = args(2)
  val epsilon = args(3).toDouble
  val deltaInDays = args(4).toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(5))
  val bloomFilterSize = args(6).toInt
  val enableEarlyAbort = args(7).toBoolean
  val sampleSize = args(8).toInt
  val seed = args(9).toLong
  val version = "0.93" //TODO: update this if discovery algorithm changes!
  val targetDir = new File(args(1) + s"/$version/")
  targetDir.mkdir()
  val subsetValidation = true
  val interactiveIndexBuilding = false
  val dataLoader = new InputDataManager(sourceFileBinary,None)
  val queryIDs = ColumnHistoryID
    .fromJsonObjectPerLineFile(queryFile)
    .toSet
  val relaxedShiftedTemporalINDDiscovery = new RelaxedShiftedTemporalINDDiscovery(dataLoader,
    new StandardResultSerializer(targetDir,bloomFilterSize,enableEarlyAbort,sampleSize,timeSliceChoiceMethod,seed),
    epsilon,
    TimeUtil.nanosPerDay*deltaInDays,
    version,
    subsetValidation,
    bloomFilterSize,
    interactiveIndexBuilding,
    timeSliceChoiceMethod,
    enableEarlyAbort,
    seed)
  relaxedShiftedTemporalINDDiscovery.discoverForSample(queryIDs,IndexedSeq(0,1,2,3,4,5,6,7,8,9,10,15,20))
  //relaxedShiftedTemporalINDDiscovery.discoverAll(20,1)
}
