package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.{TimeSliceImpactResultSerializer, TimeSliceIndexTuningStatRow}

import java.io.{File, PrintWriter}
import java.time.Instant

object TimeSliceBorderImpactExploreMain extends App with StrictLogging{
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
  val bloomfilterSize = 2048
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
  val resultPR = new PrintWriter(targetDir.getAbsolutePath + "/pruningStats.csv")
  resultPR.println(TimeSliceIndexTuningStatRow.schema)
  val (data,_) = relaxedShiftedTemporalINDDiscovery.loadData()
  val requiredValueIndex = relaxedShiftedTemporalINDDiscovery.getRequiredValuesetIndex(data)
  val indexStructures = relaxedShiftedTemporalINDDiscovery.getIterableForTimeSliceIndices(data)
  val sampleTOQuery = relaxedShiftedTemporalINDDiscovery.getRandomSampleOfInputData(data,sampleSize)
  val rqIndex = new MultiLevelIndexStructure(requiredValueIndex,new MultiTimeSliceIndexStructure(collection.SortedMap(),IndexedSeq(),false,1),0)
  val requiredIndexResults = sampleTOQuery
    .map{case (c,_) => (c,rqIndex.queryRequiredValuesIndex(c)._1)}
    .toMap
  indexStructures
    .foreach {case (timePeriod,index) =>
      logger.debug(s"beginning $timePeriod")
      val indexMap = collection.mutable.TreeMap[(Instant, Instant), BloomfilterIndex]() ++ Seq((timePeriod, index))
      val curTimeSliceIndex = new MultiTimeSliceIndexStructure(indexMap,IndexedSeq(),false,1)
      val multiLevelIndexStructure = new MultiLevelIndexStructure(requiredValueIndex,curTimeSliceIndex,0)
      sampleTOQuery.foreach{case (query,queryNum) => {
        val candidatesRequiredValues = requiredIndexResults(query)
        val (curCandidates, _) = multiLevelIndexStructure.queryTimeSliceIndices(query, candidatesRequiredValues)
        val statRow = new TimeSliceIndexTuningStatRow(queryNum,timePeriod._1,timePeriod._2,candidatesRequiredValues.count(),curCandidates.count())
        resultPR.println(statRow.toCSVLine)
        resultPR.flush()
      }}
      data.histories.foreach(eh => eh.clearTimeWindowCache())
    }
  resultPR.close()

}
