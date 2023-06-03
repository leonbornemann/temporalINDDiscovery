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

  //0.00066 0,775743
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  println(GLOBAL_CONFIG.totalTimeInNanos)
  val queryFiles = args(0).split(",").toIndexedSeq
  val targetRootDir = args(1)
  val sourceFileBinary = args(2)
  val relativeEpsilons = args(3).split(",").map(_.toDouble)
  val maxDeltasWhileIndexing = args(4).split(",").map(_.toLong)
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(args(5))
  val bloomFilterSizes = args(6).split(",").map(_.toInt).toIndexedSeq
  val seeds = args(7).split(",").map(_.toLong).toIndexedSeq
  val numTimeSliceIndicesToTest = args(8).split(",").map(_.toInt)
  val numThreadss = args(9).split(",").map(_.toInt)
  val metaDataDir = new File(args(10))
  val indexEpsilonFactors = args(11).split(",").map(_.toInt).toIndexedSeq
  val indexDeltaFactors = args(12).split(",").map(_.toInt).toIndexedSeq
  val inputSizeFactors = args(13).split(",").map(_.toDouble).toIndexedSeq
  metaDataDir.mkdirs()

  val version = "0.99"
  val targetDir = new File(targetRootDir + s"/$version/")
  targetDir.mkdir()
  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary,None)

  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
    version,
    subsetValidation,
    timeSliceChoiceMethod,
    numThreadss(0),
    metaDataDir)
  relaxedShiftedTemporalINDDiscovery.initData()
  ParallelExecutionHandler.initContext(numThreadss.max)
  for(bloomFilterSize <- bloomFilterSizes){
    logger.debug(s"Processing bloomFilterSize $bloomFilterSize")
    for (seed <- seeds) {
      logger.debug(s"Processing seed $seed")
      for(relativeEpsilon <- relativeEpsilons){
        logger.debug(s"Processing relative Epsilon $relativeEpsilon")
        for(maxDeltaWhileIndexing <- maxDeltasWhileIndexing){
          logger.debug(s"Processing max Delta $maxDeltaWhileIndexing")
          indexEpsilonFactors.foreach(epsilonFactor => {
            logger.debug(s"Processing epsilonFactor $epsilonFactor")
            indexDeltaFactors.foreach(deltaFactor => {
              logger.debug(s"Processing deltaFactor $deltaFactor")
              val absoluteExpectedEpsilon = relativeEpsilon * GLOBAL_CONFIG.totalTimeInNanos
              val expectedOmega = new ConstantWeightFunction()
              val maxDeltaInNanos = TimeUtil.nanosPerDay * maxDeltaWhileIndexing
              val queryParameters = TINDParameters(absoluteExpectedEpsilon, maxDeltaInNanos, expectedOmega)
              val indexDelta = maxDeltaInNanos * deltaFactor
              val indexEpsilon = absoluteExpectedEpsilon * epsilonFactor
              val indexParameter = TINDParameters(indexEpsilon, indexDelta, expectedOmega)
              for (inputSizeFactor <- inputSizeFactors) {
                logger.debug(s" Running inputSizeFactor $inputSizeFactor")
                relaxedShiftedTemporalINDDiscovery.useSubsetOfData(inputSizeFactor)
                relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndicesToTest.max, seed, bloomFilterSize, indexParameter)
                for (nThreads <- numThreadss) {
                  logger.debug(s" Running nThreads $nThreads")
                  val resultDirPrefix = s"${bloomFilterSize}_${seed}_${epsilonFactor}_${deltaFactor}_${nThreads}_${inputSizeFactor}_${relativeEpsilon}_$maxDeltaWhileIndexing"
                  relaxedShiftedTemporalINDDiscovery.nThreads = nThreads
                  ParallelExecutionHandler.initContext(nThreads)
                  queryFiles.foreach(queryFile => {
                    logger.debug(s"Processing queryFile $queryFile")
                    val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(queryFile), timeSliceChoiceMethod, Some(resultDirPrefix))
                    relaxedShiftedTemporalINDDiscovery.tINDSearch(new File(queryFile), numTimeSliceIndicesToTest, queryParameters, resultSerializer)
                  })
                }
              }
            })
          })
        }
      }
    }
  }
  ParallelExecutionHandler.service.shutdown()

}
