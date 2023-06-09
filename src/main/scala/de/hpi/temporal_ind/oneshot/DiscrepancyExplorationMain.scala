package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.file_search.IndexedColumnHistories
import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory
import de.hpi.temporal_ind.data.ind.weight_functions.ConstantWeightFunction
import de.hpi.temporal_ind.data.ind.{EpsilonOmegaDeltaRelaxedTemporalIND, INDCandidateIDs, ValidationVariant}
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.{ParallelExecutionHandler, TINDParameters, TINDSearcher}
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.discovery.statistics_and_results.StandardResultSerializer
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File

object DiscrepancyExplorationMain extends App {
  ParallelExecutionHandler.initContext(12)
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val path1 = "/home/leon/data/temporalINDDiscovery/finalExperiments/reverseSearchNew"
  val path2 = "/home/leon/data/temporalINDDiscovery/finalExperiments/reverseSearcPrev"
  val candidatesOld = INDCandidateIDs.fromJsonObjectPerLineFile(path1)
    .toSet
  val candidatesNew = INDCandidateIDs.fromJsonObjectPerLineFile(path2)
    .toSet
  println("in new but not in old",candidatesNew.diff(candidatesOld).size)
  candidatesNew.diff(candidatesOld).take(10).foreach(println)
  println("in old but not in new",candidatesOld.diff(candidatesNew).size)
  candidatesOld.diff(candidatesNew).take(10).foreach(println)
//  val f1 = IndexedColumnHistories.getFileForID(new File("/home/leon/data/temporalINDDiscovery/wikipedia/columnHistories"),49550066L)
//  val f2 = IndexedColumnHistories.getFileForID(new File("/home/leon/data/temporalINDDiscovery/wikipedia/columnHistories"),37099048L)
//  println(f1)
//  println(f2)
//  //TODO: check of they hold
//  //0.001485149
//  //90
//  val toTest = candidatesNew.diff(candidatesOld).take(2) ++ candidatesOld.diff(candidatesNew).take(2)
//  toTest.foreach(println(_))
//  val ids = toTest.flatMap(c => Seq(c.lhs,c.rhs))
//  val dm = new InputDataManager("",Some(IndexedSeq(new File("/home/leon/data/temporalINDDiscovery/wikipedia/newHistoriesFromServerForDebug/"))))
//  dm.setFilter(och => ids.contains(och.columnHistoryID))
//  val params = TINDParameters(0.001485149*GLOBAL_CONFIG.totalTimeInNanos,90*TimeUtil.nanosPerDay,new ConstantWeightFunction)
//  val searcher = new TINDSearcher(dm,params,"test",true,4096,TimeSliceChoiceMethod.RANDOM,12,new File("/home/leon/data/temporalINDDiscovery/wikipedia/tmp/meta/"))
//  val serializer = new StandardResultSerializer(new File("/home/leon/data/temporalINDDiscovery/wikipedia/tmp/"),new File("query"),TimeSliceChoiceMethod.RANDOM)
//  searcher.discoverForSample(toTest.map(_.lhs),IndexedSeq(20),params)
//  ParallelExecutionHandler.service.shutdown()
//  val idToHistories = dm.loadData()
//    .map(och => (och.id,och))
//    .toMap
//  toTest.foreach(c => {
//    val tind = new EpsilonOmegaDeltaRelaxedTemporalIND(idToHistories(c.lhsColumnID),idToHistories(c.rhsColumnID),params,ValidationVariant.FULL_TIME_PERIOD)
//    if(!tind.isValid){
//      println("whaat?")
//    } else {
//      println("ok")
//    }
//  })

}
