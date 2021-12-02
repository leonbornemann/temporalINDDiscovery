package de.hpi.temporal_ind.data.column

import java.io.File
import java.time.{Instant, LocalDate}
import scala.util.Random

object TableExportForManyMain extends App {
  val inputDir = args(0)
  val outputDirRootDir = args(1)

  val inputTimestamp = Instant.parse(args(2))
  val outputDir = outputDirRootDir + s"/$inputTimestamp/"
//  val randomNumberOfFilesToChoose = 10
//  val random = new Random(13)
//  val files = random.shuffle(new File(inputDir).listFiles().toIndexedSeq).take(randomNumberOfFilesToChoose)
  val files = new File(inputDir).listFiles()
  files.foreach(f => {
    val vhs = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val resultDir = new File(s"$outputDir/${f.getName}/")
    resultDir.mkdir()
    vhs.zipWithIndex
      .groupBy(t => (t._1.pageID,t._1.tableId))
      .foreach{case ((pID,tID),colList) =>
        val headerMap = colList.map(t => (t._1,t._1.id)).toMap
        val colListSorted = colList.sortBy(_._2).toIndexedSeq
        val headersOrdered = colListSorted.map(t => headerMap(t._1))
        ColumnVersion.serializeToTable(colListSorted.map(_._1.versionAt(inputTimestamp)),headersOrdered,new File(resultDir + s"/${tID}_$pID.csv"))
      }
  })

}
