package de.hpi.temporal_ind.data.column

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.original.{ColumnHistory, ColumnVersion}
import de.hpi.temporal_ind.data.column.statistics.ColumnHistoryStatRow
import de.hpi.temporal_ind.data.wikipedia.{GLOBAL_CONFIG, WikipediaDataPreparer}

import java.io.{File, PrintWriter}
import java.time.Instant

object ExportForManyMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputDir = args(1)
  val outputRootDirLineages = args(2)
  val outputDirRootDir = args(3)
  val inputTimestamp = Instant.parse(args(4))
  val outputDir = outputDirRootDir + s"/$inputTimestamp/"
  //FILTER CONFIG:
  val minMedianSize = 5
  val minLifetimeDays = 30
  val minNonDeleteChanges = 10
  //END FILTER CONFIG
  val files = new File(inputDir).listFiles()
  val preparer = new WikipediaDataPreparer()
  files
    .zipWithIndex
    .foreach{ case (f,i) =>
      logger.debug(s"Processing ${f.getAbsolutePath} ($i/${files.size}")
      val resultDir = new File(s"$outputDir/${f.getName}/")
      resultDir.mkdirs()
      val resultWriterFilteredHistory = new PrintWriter(s"$outputRootDirLineages/${f.getName}.json")
      val vhs = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
      val filtered = vhs.zipWithIndex
        .filter(vhs => {
          val stats = ColumnHistoryStatRow(vhs._1)
          !vhs._1.versionAt(inputTimestamp).isDelete &&
            stats.sizeStatistics.median>=minMedianSize &&
            stats.lifetimeInDays >= minLifetimeDays &&
            vhs._1.versionsWithNonDeleteChanges.size >= minNonDeleteChanges &&
            !preparer.mostlyNumeric(vhs._1) &&
            !preparer.isNumeric(vhs._1.columnVersions.findLast(!_.isDelete).get.values)
        })
      //serialize to filtered outputDir:
      filtered.foreach{case (ch,_) => ch.appendToWriter(resultWriterFilteredHistory)}
      resultWriterFilteredHistory.close()
      filtered
        .groupBy(t => (t._1.pageID,t._1.tableId))
        .foreach{case ((pID,tID),colList) =>
          val headerMap = colList.map(t => (t._1,t._1.id)).toMap
          val colListSorted = colList.sortBy(_._2).toIndexedSeq
          val headersOrdered = colListSorted.map(t => headerMap(t._1))
          val tableVersion = colListSorted.map(_._1.versionAt(inputTimestamp))
          if(tableVersion.exists(_.values.size!=0))
            ColumnVersion.serializeToTable(tableVersion,headersOrdered,new File(resultDir + s"/${tID}_$pID.csv"))
        }
  }

}
