package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.{TimeSliceImpactResultSerializer, TimeSliceStatisticsExtractor}

import java.io.{File, PrintWriter}

object TimeSliceStatisticsExportMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val version = "0.7_explore_time_slice" //TODO: update this if discovery algorithm changes!
  //TODO: continue adapting here!
  val sourceDirs = args(0).split(",")
    .map(new File(_))
    .toIndexedSeq
  val targetDir = new File(args(1) + s"/${version}/")
  targetDir.mkdir()
  val targetFileBinary = args(2)
  val epsilon = args(3).toDouble
  val deltaInDays = args(4).toLong
  val subsetValidation = true
  val sampleSize = 10000
  val bloomfilterSize = 4096
  val interactiveIndexBuilding = false
  val dataLoader = new InputDataManager(targetFileBinary)
  val relaxedShiftedTemporalINDDiscovery = new RelaxedShiftedTemporalINDDiscovery(dataLoader,
    new TimeSliceImpactResultSerializer(targetDir),
    epsilon,
    TimeUtil.nanosPerDay * deltaInDays,
    version,
    subsetValidation,
    bloomfilterSize,
    interactiveIndexBuilding,
    TimeSliceChoiceMethod.RANDOM,
    true)

  val resultPR = new PrintWriter(targetDir.getAbsolutePath + "/timeSliceStats.csv")
  val (data, _) = relaxedShiftedTemporalINDDiscovery.loadData()
  val absoluteEpsilonNanos = (GLOBAL_CONFIG.totalTimeInNanos * epsilon).toLong
  val allSlices = GLOBAL_CONFIG.partitionTimePeriodIntoSlices(absoluteEpsilonNanos)
  val timeSLiceStatGatherer = new TimeSliceStatisticsExtractor(data.histories.map(_.och), allSlices, resultPR)
  timeSLiceStatGatherer.extractForAll()

}
