package de.hpi.temporal_ind.data.column

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.statistics.ColumnHistoryStatRow
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.File
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDate}
import scala.util.Random

object ExportForManyMain extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val inputDir = args(0)
  val outputDirRootDir = args(1)
  val inputTimestamp = Instant.parse(args(2))
  val outputDir = outputDirRootDir + s"/$inputTimestamp/"
  //FILTER CONFIG:
  val minMedianSize = 5
  val minLifetimeDays = 30
  //END FILTER CONFIG
  val files = new File(inputDir).listFiles()
  files
    .zipWithIndex
    .foreach{ case (f,i) =>
      logger.debug(s"Processing ${f.getAbsolutePath} ($i/${files.size}")
      val resultDir = new File(s"$outputDir/${f.getName}/")
      resultDir.mkdirs()
      val vhs = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
      vhs.zipWithIndex
        .filter(vhs => {
          val stats = ColumnHistoryStatRow(vhs._1)
          stats.sizeStatistics.median>=minMedianSize && stats.lifetimeInDays >= minLifetimeDays
        })
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
