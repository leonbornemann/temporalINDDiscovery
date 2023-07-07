package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.weight_functions.{ConstantWeightFunction, TimestampWeightFunction}
import de.hpi.temporal_ind.discovery.TINDSearchMain.argMap
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File


object TINDAllPairsMain extends App with StrictLogging{
  val argsParsed = CommandLineParser(args)
  println(s"Called with ${args.toIndexedSeq}")
  argsParsed.printParams()
  val argMap = argsParsed.argMap
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  if(argMap.contains("--epsilonQueries") || argMap.contains("deltaQueries")){
    logger.error("In All pairs mode, you cannot provide --epsilonQueries or --deltaQueries, as the parameter values for the index will be used")
  } else {
    val targetRootDir = argMap("--result")
    val sourceFileBinary = argMap("--input")
    val timeSliceChoiceMethod = TimeSliceChoiceMethod.withName(argMap("--timeSliceChoice"))
    val bloomFilterSize = argMap("--m").toInt
    val seed = argMap("--seed").toLong
    val numTimeSliceIndices = argMap("--k").toInt
    val numThreads = argMap("--nThreads").toInt
    val metaDataDir = new File(argMap("--metadata"))
    val indexEpsilon = argMap("--epsilonIndex").toDouble
    val indexDelta = argMap("--deltaIndex").toLong
    val omega = TimestampWeightFunction.create(argMap("--w"),argMap.get("--alpha").map(_.toDouble))
    val reverseSearch = argMap("--reverse").toBoolean
    metaDataDir.mkdirs()
    val queryAndIndexParameters = TINDParameters(indexEpsilon, indexDelta, omega)
    val (_, time) = TimeUtil.executionTimeInMS({
      val dataLoader = new InputDataManager(sourceFileBinary, None)
      ParallelExecutionHandler.initContext(numThreads)
      val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
        true,
        timeSliceChoiceMethod,
        numThreads,
        metaDataDir,
        reverseSearch)
      relaxedShiftedTemporalINDDiscovery.initData()
      relaxedShiftedTemporalINDDiscovery.buildIndicesWithSeed(numTimeSliceIndices, seed, bloomFilterSize, queryAndIndexParameters)
      val resultDirPrefix = s"allPair"
      val resultSerializer = new StandardResultSerializer(new File(targetRootDir), new File(sourceFileBinary), timeSliceChoiceMethod, Some(resultDirPrefix))
      relaxedShiftedTemporalINDDiscovery.tINDAllPairs(numTimeSliceIndices, queryAndIndexParameters, resultSerializer)
    })
    logger.debug(s"Finished entire program in $time ms")
    ParallelExecutionHandler.service.shutdown()
  }
}
