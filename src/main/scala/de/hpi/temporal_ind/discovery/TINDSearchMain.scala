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
import java.util.UUID


object TINDSearchMain extends App with StrictLogging{
  val argsParsed = CommandLineParser(args)
  println(s"Called with ${args.toIndexedSeq}")
  argsParsed.printParams()
  val argMap = argsParsed.argMap
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays)
  println(GLOBAL_CONFIG.totalTimeInNanos)
  val queryFile = argMap("--queries")
  val targetRootDir = argMap("--result")
  val sourceFileBinary = argMap("--input")
  val queryEpsilon = argMap("--epsilonQueries").toDouble
  val queryDelta = argMap("--deltaQueries").toLong
  val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(argMap("--timeSliceChoice"))
  val bloomFilterSize = argMap("--m").toInt
  val seed = argMap("--seed").toLong
  val numTimeSliceIndices = argMap("--k").toInt
  val numThreads = argMap("--nThreads").toInt
  val metaDataDir = new File(argMap("--metadata"))
  val indexEpsilon = argMap("--epsilonIndex").toDouble
  val indexDelta = argMap("--deltaIndex").toLong
  var inputSize = argMap.get("--n").map(s => s.toInt).getOrElse(-1)
  val reverseSearch = argMap("--reverse").toBoolean
  metaDataDir.mkdirs()

  val subsetValidation = true
  val dataLoader = new InputDataManager(sourceFileBinary, None)

  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
    subsetValidation,
    timeSliceChoiceMethod,
    numThreads,
    metaDataDir,
    reverseSearch)
  relaxedShiftedTemporalINDDiscovery.initData()
  if (inputSize == -1) {
    inputSize = relaxedShiftedTemporalINDDiscovery.historiesEnrichedOriginal.histories.size
  }
  ParallelExecutionHandler.initContext(numThreads)
  val expectedOmega = new ConstantWeightFunction()
  val queryParameters = TINDParameters(queryEpsilon, queryDelta, expectedOmega)
  val indexParameter = TINDParameters(indexEpsilon, indexDelta, expectedOmega)
  relaxedShiftedTemporalINDDiscovery.useSubsetOfData(inputSize)
  relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndices, seed, bloomFilterSize, indexParameter)
  val resultDirPrefix = UUID.randomUUID().toString
  relaxedShiftedTemporalINDDiscovery.nThreads = numThreads
  ParallelExecutionHandler.initContext(numThreads)
  val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(queryFile), timeSliceChoiceMethod, Some(resultDirPrefix))
  relaxedShiftedTemporalINDDiscovery.tINDSearch(new File(queryFile), IndexedSeq(numTimeSliceIndices), queryParameters, resultSerializer)
  ParallelExecutionHandler.service.shutdown()

}
