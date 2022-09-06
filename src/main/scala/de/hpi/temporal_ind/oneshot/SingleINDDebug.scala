package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.column.data.original.{INDCandidate, LabelledINDCandidateStatistics}
import de.hpi.temporal_ind.data.column.labelling.TINDCandidateMetricDisagreementExport.deltas
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object SingleINDDebug extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  //val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/filteredHistories/")
  val dir = new File("/home/leon/data/temporalINDDiscovery/wikipedia/debugHistories/wikipediaFilteredNewUnion")
  //val candidate = InclusionDependencyFromMany.fromManyOutputString("[386460726-1_27535007.csv.b989e698-860c-4cd4-822d-646fdd8ee50b][=[386460726-1_27535007.csv.c01075bf-14bd-47dd-8a31-a730fcc84326]")
  val candidate = InclusionDependencyFromMany.fromManyOutputString("[707928358-0_49621038.csv.09184664-d0ff-4c3b-b41b-b6a427e52cac][=[694657066-3_48784263.csv.93a90120-656c-4796-a9d1-9b6340bd494f_union]")
  //[707928358-0_49621038.csv.09184664-d0ff-4c3b-b41b-b6a427e52cac][=[694657066-3_48784263.csv.93a90120-656c-4796-a9d1-9b6340bd494f_union]
  //48520749
  //49621038
  //48819209
  val index = IndexedColumnHistories.loadForPageIDS(dir,IndexedSeq(candidate.rhsPageID.toLong,candidate.lhsPageID.toLong))
  val pr = new PrintWriter("src/main/resources/tmp/tmpLabel.txt")
  val labelled = candidate
    .toCandidate(index,true)
    .toLabelledINDCandidateStatistics("label")
  println(labelled)
  val deltas = Seq(TimeUtil.nanosPerDay*7,
    TimeUtil.nanosPerDay*10,
    TimeUtil.nanosPerDay*30,
    TimeUtil.nanosPerDay*60,
    TimeUtil.nanosPerDay*90,
    TimeUtil.nanosPerDay*365)
  val executionTimes = ( 0 until 100).map{ _ =>
    val time = System.currentTimeMillis()
    val res = deltas.exists(d => {
      if(d==TimeUtil.nanosPerDay*365)
        println()
      labelled.simpleAndComplexAreDifferentForDelta(d)
    })
    val timeAfter = System.currentTimeMillis()
    val finalTime = (timeAfter-time) / 1000.0
    println(res,finalTime)
    finalTime
  }
  println("AVG Time:",executionTimes.sum / executionTimes.size.toDouble,"s")
  labelled.serializeValidityStatistics(pr)
  pr.close()

}
