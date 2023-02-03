package de.hpi.temporal_ind.data.column.data.many

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.ColumnHistoryMetadata
import de.hpi.temporal_ind.data.column.data.original.INDCandidate
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.{File, PrintWriter}

object MANYINDFilter extends App with StrictLogging{
  println(s"called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = args(0)
  val metadataFile = args(1)
  val outputDirToLabel = args(2)
  new File(outputDirToLabel).mkdirs()
  val map = ColumnHistoryMetadata.readAsMap(metadataFile)
  new File(inputDir).listFiles().foreach(inputFile => {
    logger.debug(s"processing $inputFile")
    val outputFileToLabel = outputDirToLabel + s"/${inputFile.getName}.json"
    val pr = new PrintWriter(outputFileToLabel)
    InclusionDependencyFromMany.readFromMANYOutputFile(inputFile)
      .withFilter(tind => map(tind.lhsColumnID).medianSize>=5 && map(tind.rhsColumnID).medianSize>=5)
      .foreach(candidate => {
        pr.println(candidate.toMANYString)
      })
    pr.close()
  })
}
