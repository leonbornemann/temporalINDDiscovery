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
  val (beginTimestamp,endTimestampExclusive) = if(args(4).contains(";"))
    (Instant.parse(args(4).split(";")(0)),Some(Instant.parse(args(4).split(";")(1))))
  else {
    (Instant.parse(args(4)),None)
  }
  val outputDir = outputDirRootDir + s"/${beginTimestamp}_$endTimestampExclusive/"
  val versonRangeEndExlcusive = endTimestampExclusive.getOrElse(beginTimestamp.plusNanos(1))
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
          val nonDelete:Boolean = if(endTimestampExclusive.isEmpty)
            !vhs._1.versionAt(beginTimestamp).isDelete
          else
            vhs._1.existsNonDeleteInVersionRange(beginTimestamp,endTimestampExclusive.get)
          nonDelete &&
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
          val tableVersion = if(!endTimestampExclusive.isDefined)
            colListSorted.map(_._1.versionAt(beginTimestamp))
          else
            colListSorted.map(_._1.versionUnion(beginTimestamp,endTimestampExclusive.get))
          if(tableVersion.exists(_.values.size!=0))
            ColumnVersion.serializeToTable(tableVersion,headersOrdered,new File(resultDir + s"/${tID}_$pID.csv"))
        }
  }

}
