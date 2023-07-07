package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.{ConstantWeightFunction, TimestampWeightFunction}
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File
import java.util.UUID

object TINDRuntimeExperiments extends App with StrictLogging{
  val argsParsed = CommandLineParser(args)
  println(s"Called with ${args.toIndexedSeq}")
  argsParsed.printParams()
  val argMap = argsParsed.argMap
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  println(GLOBAL_CONFIG.totalTimeInNanos)
  val queryFiles = argMap("--queries").split(",").toIndexedSeq
  val targetRootDir = argMap("--result")
  val sourceFileBinary = argMap("--input")
  val queryEpsilons = argMap("--epsilonQueries").split(",").map(_.toDouble)
  val queryDeltas = argMap("--deltaQueries").split(",").map(_.toLong)
  val omega = TimestampWeightFunction.create(argMap("--w"),argMap.get("--alpha").map(_.toDouble))
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(argMap("--timeSliceChoice"))
  val bloomFilterSizes = argMap("--m").split(",").map(_.toInt).toIndexedSeq
  val seeds = argMap("--seed").split(",").map(_.toLong).toIndexedSeq
  val numTimeSliceIndicesToTest = argMap("--k").split(",").map(_.toInt)
  val numThreadss = argMap("--nThreads").split(",").map(_.toInt)
  val metaDataDir = new File(argMap("--metadata"))
  val indexEpsilons = argMap("--epsilonIndex").split(",").map(_.toDouble).toIndexedSeq
  val indexDeltas = argMap("--deltaIndex").split(",").map(_.toLong).toIndexedSeq
  var inputSizes = argMap.getOrElse("--n", "").split(",").map(_.toInt).toIndexedSeq
  val reverseSearch = argMap("--reverse").toBoolean
  metaDataDir.mkdirs()

  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary, None)

  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
    subsetValidation,
    timeSliceChoiceMethod,
    numThreadss(0),
    metaDataDir,
    reverseSearch)
  relaxedShiftedTemporalINDDiscovery.initData()
  if (inputSizes.isEmpty) {
    inputSizes = IndexedSeq(relaxedShiftedTemporalINDDiscovery.historiesEnrichedOriginal.histories.size)
  }
  ParallelExecutionHandler.initContext(numThreadss.max)
  for (bloomFilterSize <- bloomFilterSizes) {
    logger.debug(s"Processing bloomFilterSize $bloomFilterSize")
    for (seed <- seeds) {
      logger.debug(s"Processing seed $seed")
      for (absQueryEpsilon <- queryEpsilons) {
        logger.debug(s"Processing absolute Epsilon $absQueryEpsilon")
        for (maxDeltaInNanos <- queryDeltas) {
          logger.debug(s"Processing max Delta $maxDeltaInNanos")
          indexEpsilons.foreach(indexEpsilon => {
            logger.debug(s"Processing epsilonFactor $indexEpsilon")
            indexDeltas.foreach(indexDelta => {
              logger.debug(s"Processing deltaFactor $indexDelta")
              println("Abs Epsilon:", absQueryEpsilon)
              val queryParameters = TINDParameters(absQueryEpsilon, maxDeltaInNanos, omega)
              val indexParameter = TINDParameters(indexEpsilon, indexDelta, omega)
              for (inputSize <- inputSizes) {
                logger.debug(s" Running inputSize $inputSize")
                relaxedShiftedTemporalINDDiscovery.useSubsetOfData(inputSize)
                relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndicesToTest.max, seed, bloomFilterSize, indexParameter)
                for (nThreads <- numThreadss) {
                  logger.debug(s" Running nThreads $nThreads")
                  val resultDirPrefix = UUID.randomUUID().toString
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
