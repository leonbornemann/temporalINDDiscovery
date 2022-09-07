package de.hpi.temporal_ind.data.column.labelling

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.column.data.original.INDCandidate
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object TINDCandidateMetricDisagreementExport extends App with StrictLogging{
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputFile = args(1)
  val columnHistoryDir = args(2)
  val outputFileToLabel = args(3)
  val filterByUnion = args(4).toBoolean
  //val outputFileStatistics = args(4)
  val version = GLOBAL_CONFIG.lastInstant
  val sampleSize = 2000
  val pr = new PrintWriter(outputFileToLabel)
  pr.println(INDCandidate.csvSchema)
  val indexed = IndexedColumnHistories.fromColumnHistoryJsonPerLineDir(columnHistoryDir)
  var discarded = 0
  var found = 0
  val deltas = Seq(TimeUtil.nanosPerDay*365)
  InclusionDependencyFromMany.readFromMANYOutputFile(new File(inputFile))
    .withFilter(tind => !filterByUnion || (tind.rhsColumnID.contains("union") && !tind.lhsColumnID.contains("union")))
    .map(ind => ind.toCandidate(indexed,filterByUnion))
    .foreach(candidate => {
      if(deltas.exists(d => candidate.toLabelledINDCandidateStatistics("null").simpleAndComplexAreDifferentForDelta(d))){
        found +=1
        pr.println(candidate.toLabelCSVString(version))
        pr.flush()
      } else {
        discarded+=1
      }
      if((found+discarded)%1000==0)
        logger.debug(s"Processed ${found+discarded}, found $found")
      if(found%10==0)
        logger.debug(s"Processed ${found+discarded}, found $found")
    })
  pr.close()
}
