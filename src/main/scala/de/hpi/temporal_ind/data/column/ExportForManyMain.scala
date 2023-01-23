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
  val inputRootDir = new File(args(1))
  val outputDirRootDir = args(2)
  val (beginTimestamp,endTimestampExclusive) = if(args(3).contains(";"))
    (Instant.parse(args(3).split(";")(0)),Some(Instant.parse(args(3).split(";")(1))))
  else {
    (Instant.parse(args(3)),None)
  }
  inputRootDir.listFiles().foreach(inputDir => {
    println(s"Processing $inputDir")
    val outputDir = outputDirRootDir + s"/${inputDir.getName}/"
    val files = inputDir.listFiles()
    val preparer = new WikipediaDataPreparer()
    files
      .zipWithIndex
      .foreach { case (f, i) =>
        logger.debug(s"Processing ${f.getAbsolutePath} ($i/${files.size}")
        val resultDir = new File(s"$outputDir/${f.getName}/")
        resultDir.mkdirs()
        val vhs = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
        val filtered = vhs.zipWithIndex
          .filter(vhs => {
            val nonDelete: Boolean = if (endTimestampExclusive.isEmpty)
              !vhs._1.versionAt(beginTimestamp).isDelete
            else
              vhs._1.existsNonDeleteInVersionRange(beginTimestamp, endTimestampExclusive.get)
            nonDelete &&
              !preparer.mostlyNumeric(vhs._1) &&
              !preparer.isNumeric(vhs._1.columnVersions.findLast(!_.isDelete).get.values)
          })
        //serialize to filtered outputDir:
        filtered.foreach { case (ch, _) => ch.appendToWriter(resultWriterFilteredHistory) }
        resultWriterFilteredHistory.close()
        filtered
          .groupBy(t => (t._1.pageID, t._1.tableId))
          .foreach { case ((pID, tID), colList) =>
            val headerMap = colList.map(t => (t._1, t._1.id)).toMap
            val colListSorted = colList.sortBy(_._2).toIndexedSeq
            val headersOrdered = if (!endTimestampExclusive.isDefined)
              colListSorted.map(t => headerMap(t._1))
            else
              colListSorted.flatMap(t => Seq(headerMap(t._1) + "_union", headerMap(t._1)))
            val tableVersion = if (!endTimestampExclusive.isDefined)
              colListSorted.map(_._1.versionAt(beginTimestamp))
            else
              colListSorted.flatMap(t => Seq(t._1.versionUnion(beginTimestamp, endTimestampExclusive.get), t._1.versionAt(beginTimestamp)))
            if (tableVersion.exists(_.values.size != 0))
              ColumnVersion.serializeToTable(tableVersion, headersOrdered, new File(resultDir + s"/${tID}_$pID.csv"))
          }
      }
  })


}
