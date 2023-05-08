package de.hpi.temporal_ind.oneshot

import com.typesafe.scalalogging.StrictLogging

object TimeSliceBorderImpactExploreMain extends App with StrictLogging{
//  println(s"Called with ${args.toIndexedSeq}")
//  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
//  println(GLOBAL_CONFIG.totalTimeInDays)
//  val version = "0.7_explore_time_slice" //TODO: update this if discovery algorithm changes!
//  //TODO: continue adapting here!
//  val sourceDirs = args(0).split(",")
//    .map(new File(_))
//    .toIndexedSeq
//  val targetDir = new File(args(1) + s"/${version}/")
//  targetDir.mkdir()
//  val targetFileBinary = args(2)
//  val epsilon = args(3).toDouble
//  val deltaInDays = args(4).toLong
//  val subsetValidation = true
//  val sampleSize = 10000
//  val bloomfilterSize = 2048
//  val interactiveIndexBuilding = false
//  val dataLoader = new InputDataManager(targetFileBinary)
//  val expectedQueryParams = TINDParameters(epsilon,TimeUtil.nanosPerDay * deltaInDays,new ConstantWeightFunction())
//  val relaxedShiftedTemporalINDDiscovery = new TINDSearcher(dataLoader,
//    new TimeSliceImpactResultSerializer(targetDir),
//    expectedQueryParams,
//    version,
//    subsetValidation,
//    bloomfilterSize,
//    TimeSliceChoiceMethod.RANDOM,
//    13,
//    1)
//  val resultPR = new PrintWriter(targetDir.getAbsolutePath + "/pruningStats.csv")
//  resultPR.println(TimeSliceIndexTuningStatRow.schema)
//  val (data,_) = relaxedShiftedTemporalINDDiscovery.loadData()
//  val requiredValueIndex = relaxedShiftedTemporalINDDiscovery.getRequiredValuesetIndex(data)
//  val indexStructures = relaxedShiftedTemporalINDDiscovery.getIterableForTimeSliceIndices(data)
//  val sampleTOQuery = relaxedShiftedTemporalINDDiscovery.getRandomSampleOfInputData(data,sampleSize)
//  val rqIndex = new MultiLevelIndexStructure(requiredValueIndex,new MultiTimeSliceIndexStructure(collection.SortedMap(),IndexedSeq()),0)
//  val requiredIndexResults = sampleTOQuery
//    .map{case (c,_) => (c,rqIndex.queryRequiredValuesIndex(c,expectedQueryParams)._1)}
//    .toMap
//  indexStructures
//    .foreach {case (timePeriod,index) =>
//      logger.debug(s"beginning $timePeriod")
//      val indexMap = collection.mutable.TreeMap[(Instant, Instant), BloomfilterIndex]() ++ Seq((timePeriod, index))
//      val curTimeSliceIndex = new MultiTimeSliceIndexStructure(indexMap,IndexedSeq())
//      val multiLevelIndexStructure = new MultiLevelIndexStructure(requiredValueIndex,curTimeSliceIndex,0)
//      sampleTOQuery.foreach{case (query,queryNum) => {
//        val candidatesRequiredValues = requiredIndexResults(query)
//        val (curCandidates, _) = multiLevelIndexStructure.queryTimeSliceIndices(query,expectedQueryParams, candidatesRequiredValues)
//        val statRow = new TimeSliceIndexTuningStatRow(queryNum,timePeriod._1,timePeriod._2,candidatesRequiredValues.count(),curCandidates.count())
//        resultPR.println(statRow.toCSVLine)
//        resultPR.flush()
//      }}
//      data.histories.foreach(eh => eh.clearTimeWindowCache())
//    }
//  resultPR.close()

}
