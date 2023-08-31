package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.TimestampWeightFunction
import de.hpi.temporal_ind.discovery.TINDRuntimeExperiments.argMap
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer

import java.io.File
import java.util.UUID

object BaselineMain extends App with StrictLogging{
  val argsParsed = CommandLineParser(args)
  println(s"Called with ${args.toIndexedSeq}")
  argsParsed.printParams()
  val argMap = argsParsed.argMap
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val queryFiles = argMap("--queries").split(",").toIndexedSeq
  val targetRootDir = argMap("--result")
  val sourceFileBinary = argMap("--input")
  val bloomFilterSizes = argMap("--m").split(",").map(_.toInt).toIndexedSeq
  val seeds = argMap("--seed").split(",").map(_.toLong).toIndexedSeq
  val numThreadss = argMap("--nThreads").split(",").map(_.toInt)
  val indexEpsilons = argMap("--epsilonIndex").split(",").map(_.toDouble).toIndexedSeq
  val indexDeltas = argMap("--deltaIndex").split(",").map(_.toLong).toIndexedSeq
  var inputSizes = argMap.getOrElse("--n", "").split(",").map(_.toInt).toIndexedSeq
  val numTimestampIndicesToBuild = argMap("--k").split(",").map(_.toInt)


  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary, None)

  val baselineINDDiscovery = new BaselineTemporalINDDiscovery(dataLoader,
    subsetValidation,
    numThreadss(0))
  if (inputSizes.isEmpty) {
    inputSizes = IndexedSeq(baselineINDDiscovery.historiesEnrichedOriginal.histories.size)
  }
//  println("asb Epsilon:", 0.00066*GLOBAL_CONFIG.totalTimeInNanos)
//  println("asb Epsilon:", 0.00066*GLOBAL_CONFIG.totalTimeInDays)
//  println("asb Delta:", GLOBAL_CONFIG.totalTimeInNanos*7)
  baselineINDDiscovery.initData()
  ParallelExecutionHandler.initContext(numThreadss.max)
  for (bloomFilterSize <- bloomFilterSizes) {
    logger.debug(s"Processing bloomFilterSize $bloomFilterSize")
    for (seed <- seeds) {
      logger.debug(s"Processing seed $seed")
      indexEpsilons.foreach(indexEpsilon => {
        logger.debug(s"Processing epsilonFactor $indexEpsilon")
        indexDeltas.foreach(indexDelta => {
          logger.debug(s"Processing deltaFactor $indexDelta")
          val indexParameter = TINDParameters(indexEpsilon, indexDelta,  TimestampWeightFunction.create("constant",None))
          for (inputSize <- inputSizes) {
            logger.debug(s" Running inputSize $inputSize")
            baselineINDDiscovery.useSubsetOfData(inputSize)
            baselineINDDiscovery.buildIndicesWithSeed(numTimestampIndicesToBuild.max, seed, bloomFilterSize, indexParameter)
            baselineINDDiscovery.completePruningFailTimes.clear()
            for (nThreads <- numThreadss) {
              logger.debug(s" Running nThreads $nThreads")
              val resultDirPrefix = UUID.randomUUID().toString
              baselineINDDiscovery.nThreads = nThreads
              ParallelExecutionHandler.initContext(nThreads)
              println("indexParameter.absoluteEpsilon",indexParameter.absoluteEpsilon)
              println("indexParameter.delta",indexParameter.absDeltaInNanos)
              queryFiles.foreach(queryFile => {
                logger.debug(s"Processing queryFile $queryFile")
                val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(queryFile), TimeSliceChoiceMethod.RANDOM, Some(resultDirPrefix))
                baselineINDDiscovery.tINDSearch(new File(queryFile), numTimestampIndicesToBuild.max, resultSerializer)
              })
            }
          }
        })
      })
    }
  }
  ParallelExecutionHandler.service.shutdown()

}
