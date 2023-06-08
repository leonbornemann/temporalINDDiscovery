package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File


object TINDAllPairsMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val targetRootDir = args(0)
  val sourceFileBinary = args(1)
  val relativeEpsilon = args(2).toDouble
  val maxDeltaWhileIndexing = args(3).toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(4))
  val bloomFilterSize = args(5).toInt
  val seed = args(6).toLong
  val numTimeSliceIndices = args(7).toInt
  val numThreadss = args(8).toInt
  val metaDataDir = new File(args(9))
  val useReverseSearch = args(10).toBoolean
  metaDataDir.mkdirs()
  val absoluteExpectedEpsilon = relativeEpsilon * GLOBAL_CONFIG.totalTimeInNanos
  val expectedOmega = new ConstantWeightFunction()
  val maxDeltaInNanos = TimeUtil.nanosPerDay * maxDeltaWhileIndexing
  val queryAndIndexParameters = TINDParameters(absoluteExpectedEpsilon, maxDeltaInNanos, expectedOmega)
  val version = "0.99"
  val subsetValidation = true
  val (_,time) = TimeUtil.executionTimeInMS({
    val dataLoader = new InputDataManager(sourceFileBinary, None)
    ParallelExecutionHandler.initContext(numThreadss)
    val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
      version,
      subsetValidation,
      timeSliceChoiceMethod,
      numThreadss,
      metaDataDir,
      useReverseSearch)
    relaxedShiftedTemporalINDDiscovery.initData()
    relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndices, seed, bloomFilterSize, queryAndIndexParameters)
    val resultDirPrefix = s"allPair_${bloomFilterSize}_${seed}_${numThreadss}"
    val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(sourceFileBinary), timeSliceChoiceMethod, Some(resultDirPrefix))
    relaxedShiftedTemporalINDDiscovery.tINDAllPairs(numTimeSliceIndices, queryAndIndexParameters, resultSerializer)
  })
  logger.debug(s"Finished entire program in $time ms")

  //
  //            for (nThreads <- numThreadss) {
  //              logger.debug(s" Running nThreads $nThreads")
  //              val resultDirPrefix = s"${bloomFilterSize}_${seed}_${epsilonFactor}_${deltaFactor}_${nThreads}_$inputSizeFactor"
  //              relaxedShiftedTemporalINDDiscovery.nThreads = nThreads
  //              ParallelExecutionHandler.initContext(nThreads)
  //              queryFiles.foreach(queryFile => {
  //                logger.debug(s"Processing queryFile $queryFile")
  //                val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(queryFile), timeSliceChoiceMethod, Some(resultDirPrefix))
  //                relaxedShiftedTemporalINDDiscovery.tINDSearch(new File(queryFile), numTimeSliceIndicesToTest, queryParameters, resultSerializer)
  //              })

  ParallelExecutionHandler.service.shutdown()
}
