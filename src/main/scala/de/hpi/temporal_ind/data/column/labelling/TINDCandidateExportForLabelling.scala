package de.hpi.temporal_ind.data.column.labelling

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.column.data.original.{INDCandidate, LabelledINDCandidateStatistics}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object TINDCandidateExportForLabelling extends App {
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputDir = args(1)
  val columnHistoryDir = args(2)
  val outputDirToLabel = args(3)
  val filterByUnion = args(4).toBoolean
  //val outputFileStatistics = args(4)
  val version = GLOBAL_CONFIG.lastInstant
  val sampleSize = 2000
  val indexed = IndexedColumnHistories.fromColumnHistoryJsonPerLineDir(columnHistoryDir)
  new File(inputDir).listFiles().foreach(inputFile => {
    val outputFileToLabel = outputDirToLabel + s"/${inputFile.getName}.json"
    val pr = new PrintWriter(outputFileToLabel)
    pr.println(INDCandidate.csvSchema)
    InclusionDependencyFromMany.readFromMANYOutputFile(inputFile)
      .withFilter(tind => !filterByUnion || (tind.rhsColumnID.contains("union") && !tind.lhsColumnID.contains("union") && tind.rhsColumnID.replace("_union", "") != tind.lhsColumnID))
      .take(sampleSize)
      .map(ind => ind.toCandidate(indexed, filterByUnion))
      .foreach(candidate => {
        pr.println(candidate.toLabelCSVString(version))
      })
    pr.close()
  })

}
