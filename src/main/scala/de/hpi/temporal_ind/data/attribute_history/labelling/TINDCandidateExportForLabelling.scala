package de.hpi.temporal_ind.data.attribute_history.labelling

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.file_search.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.attribute_history.data.many.InclusionDependencyFromMany
import de.hpi.temporal_ind.data.attribute_history.data.metadata.LabelledINDCandidateStatistics
import de.hpi.temporal_ind.data.ind.INDCandidate

import java.io.{File, PrintWriter}

object TINDCandidateExportForLabelling extends App with StrictLogging{
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputDir = args(1)
  val columnHistoryDir = args(2)
  val outputDirToLabel = args(3)
  //val outputFileStatistics = args(4)
  val version = GLOBAL_CONFIG.lastInstant
  val sampleSize = 120
  val index = new IncrementalIndexedColumnHistories(new File(columnHistoryDir))
  new File(inputDir).listFiles().foreach(inputFile => {
    logger.debug(s"processing $inputFile")
    val outputFileToLabel = outputDirToLabel + s"/${inputFile.getName}.json"
    val pr = new PrintWriter(outputFileToLabel)
    pr.println(INDCandidate.csvSchema)
    val allINds =InclusionDependencyFromMany.readFromMANYOutputFile(inputFile)
    for(i <- 0 until 9*300)
      allINds.next()
    allINds
      .take(sampleSize)
      .map(ind => ind.toCandidateWithIncrementalIndex(index, false))
      .foreach(candidate => {
        pr.println(candidate.toLabelCSVString(version))
        pr.flush()
      })
    pr.close()
  })

}
