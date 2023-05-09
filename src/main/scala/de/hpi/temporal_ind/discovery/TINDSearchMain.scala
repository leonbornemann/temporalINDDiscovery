package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.ColumnHistoryID
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File

object TINDSearchMain extends App {
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val queryFile = args(0)
  val targetRootDir = args(1)
  val sourceFileBinary = args(2)
  val relativeEpsilon = args(3).toDouble
  val maxDelta = args(4).toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(5))
  val bloomFilterSize = args(6).toInt
  val seed = args(7).toLong
  val numTimeSliceIndicesToTest = args(8).split(",").map(_.toInt)
  val numThreads = args(9).toInt
  val metaDataDir = new File(args(10))
  metaDataDir.mkdirs()
  val expectedEpsilon = relativeEpsilon * GLOBAL_CONFIG.totalTimeInNanos
  val expectedOmega = new ConstantWeightFunction()
  val expectedParametersWhileIndexing = TINDParameters(expectedEpsilon, TimeUtil.nanosPerDay * maxDelta, expectedOmega)
  val queryParameters = TINDParameters(expectedEpsilon, maxDelta, expectedOmega)
  val version = "0.96" //TODO: update this if discovery algorithm changes!
  val targetDir = new File(targetRootDir + s"/$version/")
  targetDir.mkdir()
  ParallelExecutionHandler.initContext(numThreads)
  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary,None)
  val queryIDs = ColumnHistoryID
    .fromJsonObjectPerLineFile(queryFile)
    .toSet
  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
    new File(queryFile),
    targetDir,
    expectedParametersWhileIndexing,
    version,
    subsetValidation,
    bloomFilterSize,
    timeSliceChoiceMethod,
    seed,
    numThreads,
    metaDataDir)
  relaxedShiftedTemporalINDDiscovery.discoverForSample(queryIDs,numTimeSliceIndicesToTest,queryParameters)
  ParallelExecutionHandler.service.shutdown()
  //relaxedShiftedTemporalINDDiscovery.discoverAll(20,1)
}
