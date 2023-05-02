package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, INDCandidateIDs, ShifteddRelaxedCustomFunctionTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.input_data.InputDataManager

object DiscrepancyExplorationMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val path1 = "/home/leon/data/temporalINDDiscovery/finalExperiments/columnHistories/testOutput/null/4092_RANDOM_13__discoveredINDs.jsonl"
  val path2 = "/home/leon/data/temporalINDDiscovery/finalExperiments/columnHistories/testOutput/0.93/4092_true_10000_RANDOM_13__discoveredINDs.jsonl"
  val candidatesNew = INDCandidateIDs.fromJsonObjectPerLineFile(path1)
    .toSet
  val candidatesOld = INDCandidateIDs.fromJsonObjectPerLineFile(path2)
    .toSet
  println("in new but not in old",candidatesNew.diff(candidatesOld).size)
  println("in old but not in new",candidatesOld.diff(candidatesNew).size)
  //TODO: check of they hold
  //0.001485149
  //90
  val dataInput = new InputDataManager("/home/leon/data/temporalINDDiscovery/finalExperiments/columnHistories/binaryTestSample.bin")
  val idToHistories = dataInput.loadData()
    .map(och => (och.id,och))
    .toMap
  val params = TINDParameters(0.001485149*GLOBAL_CONFIG.totalTimeInNanos,90*TimeUtil.nanosPerDay,new ConstantWeightFunction)
  candidatesOld.diff(candidatesNew).foreach(c => {
    val tind = new ShifteddRelaxedCustomFunctionTemporalIND(idToHistories(c.lhsColumnID),idToHistories(c.rhsColumnID),params,ValidationVariant.FULL_TIME_PERIOD)
    if(!tind.isValid){
      println("whaat?")
    } else {
      println("ok")
    }
  })

}
