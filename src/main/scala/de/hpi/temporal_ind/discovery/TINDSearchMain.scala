package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.ColumnHistoryID
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File

object TINDSearchMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  val queryFiles = args(0).split(",").toIndexedSeq
  val targetRootDir = args(1)
  val sourceFileBinary = args(2)
  val relativeEpsilon = args(3).toDouble
  val maxDeltaWhileIndexing = args(4).toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(5))
  val bloomFilterSizes = args(6).split(",").map(_.toInt).toIndexedSeq
  val seeds = args(7).split(",").map(_.toLong).toIndexedSeq
  val numTimeSliceIndicesToTest = args(8).split(",").map(_.toInt)
  val numThreads = args(9).toInt
  val metaDataDir = new File(args(10))
  val indexEpsilonFactors = args(11).split(",").map(_.toInt).toIndexedSeq
  val indexDeltaFactors = args(12).split(",").map(_.toInt).toIndexedSeq
  metaDataDir.mkdirs()
  val absoluteExpectedEpsilon = relativeEpsilon * GLOBAL_CONFIG.totalTimeInNanos
  val expectedOmega = new ConstantWeightFunction()
  val maxDeltaInNanos = TimeUtil.nanosPerDay*maxDeltaWhileIndexing
  val queryParameters = TINDParameters(absoluteExpectedEpsilon, maxDeltaInNanos, expectedOmega)
  val version = "0.99"
  val targetDir = new File(targetRootDir + s"/$version/")
  targetDir.mkdir()
  ParallelExecutionHandler.initContext(numThreads)
  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary,None)

  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
    version,
    subsetValidation,
    timeSliceChoiceMethod,
    numThreads,
    metaDataDir)
  relaxedShiftedTemporalINDDiscovery.initData()
  for(bloomFilterSize <- bloomFilterSizes){
    logger.debug(s"Processing bloomFilterSize $bloomFilterSize")
    for (seed <- seeds) {
      logger.debug(s"Processing seed $seed")
      indexEpsilonFactors.foreach(epsilonFactor => {
        logger.debug(s"Processing epsilonFactor $epsilonFactor")
        indexDeltaFactors.foreach(deltaFactor => {
          logger.debug(s"Processing deltaFactor $deltaFactor")
          val indexDelta = maxDeltaInNanos * deltaFactor
          val indexParameter = TINDParameters(absoluteExpectedEpsilon, indexDelta, expectedOmega)
          relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndicesToTest.max, seed,bloomFilterSize,indexParameter)
          val resultDirPrefix = s"${bloomFilterSize}_${seed}_${epsilonFactor}_${deltaFactor}"
          queryFiles.foreach(queryFile => {
            logger.debug(s"Processing queryFile $queryFile")
            val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(queryFile), timeSliceChoiceMethod, Some(resultDirPrefix))
            relaxedShiftedTemporalINDDiscovery.discoverForSample(new File(queryFile), numTimeSliceIndicesToTest, queryParameters, resultSerializer)
          })
        })
      })
    }
  }



  ParallelExecutionHandler.service.shutdown()

}
