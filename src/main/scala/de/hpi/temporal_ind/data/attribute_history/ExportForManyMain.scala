package de.hpi.temporal_ind.data.attribute_history

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.attribute_history.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.attribute_history.data.original.{ColumnHistory, ColumnVersion}
import de.hpi.temporal_ind.data.attribute_history.statistics.ColumnHistoryStatRow
import de.hpi.temporal_ind.data.wikipedia.WikipediaDataPreparer
import de.hpi.temporal_ind.discovery.input_data.InputDataManager

import java.io.{File, PrintWriter}
import java.time.Instant

object ExportForManyMain extends App with StrictLogging{
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dataSourceFile = args(0)
  val outputRootDir = new File(args(1))
  outputRootDir.mkdirs()
  val dm = new InputDataManager(dataSourceFile)
  val data = dm.loadData()

  def serializeToCSVs(count:Int,batchSize: Int,columnsWithID:IndexedSeq[(String,AbstractColumnVersion[String])]) = {
    columnsWithID
      .grouped(batchSize)
      .toIndexedSeq
      .zipWithIndex
      .foreach{case (batchWithID,batchID) => {
        val file = new File(outputRootDir.getAbsolutePath + s"/${count}_${batchID}")
        ColumnVersion.serializeToTable(batchWithID.map(_._2).map(_.asInstanceOf[ColumnVersion]),batchWithID.map(_._1),file)
      }}
  }

  data
    .withFilter(och => !och.versionAt(GLOBAL_CONFIG.lastInstant).isDelete)
    .map(och => (och.pageID + "_" + och.id,och.versionAt(GLOBAL_CONFIG.lastInstant)))
    .groupBy(_._2.values.size)
    .foreach{case (count,columnsWithID) => serializeToCSVs(count,100,columnsWithID)}

//  println(s"Called with ${args.toIndexedSeq}")
//  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
//  val inputRootDir = new File(args(0))
//  val outputDirRootDir = args(1)
//  val inputType = args(2)
//  val (beginTimestamp,endTimestampExclusive) = if(args(3).contains(";"))
//    (Instant.parse(args(3).split(";")(0)),Some(Instant.parse(args(3).split(";")(1))))
//  else {
//    (Instant.parse(args(3)),None)
//  }
//  val applyBasiCFilter = args(4).toBoolean
//  if(inputType=="singleDir"){
//    processInputDir(inputRootDir,outputDirRootDir + s"/${inputRootDir.getName}/")
//  } else{
//    assert(inputType=="multiDir")
//    processInputDir(inputRootDir,outputDirRootDir + s"/${inputRootDir.getName}/")
//  }
//
//  def satisfiesBasicFilter(vhs: (ColumnHistory, Int)) = {
//    ColumnHistoryStatRow(vhs._1).satisfiesBasicFilter
//  }
//
//  private def processInputDir(inputDir: File, outputDir:String): Unit = {
//    println(s"Processing $inputDir")
//    val files = inputDir.listFiles()
//    val preparer = new WikipediaDataPreparer()
//    files
//      .zipWithIndex
//      .foreach { case (f, i) =>
//        logger.debug(s"Processing ${f.getAbsolutePath} ($i/${files.size}")
//        val resultDir = new File(s"$outputDir/${f.getName}/")
//        resultDir.mkdirs()
//        val vhs = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
//        val filtered = vhs.zipWithIndex
//          .filter(vhs => {
//            val nonDelete: Boolean = if (endTimestampExclusive.isEmpty)
//              !vhs._1.versionAt(beginTimestamp).isDelete
//            else
//              vhs._1.existsNonDeleteInVersionRange(beginTimestamp, endTimestampExclusive.get)
//            nonDelete &&
//              !preparer.mostlyNumeric(vhs._1) &&
//              !preparer.isMostlyNumeric(vhs._1.columnVersions.findLast(!_.isDelete).get.values) &&
//              (!applyBasiCFilter || satisfiesBasicFilter(vhs))
//          })
//        filtered
//          .groupBy(t => (t._1.pageID, t._1.tableId))
//          .foreach { case ((pID, tID), colList) =>
//            val headerMap = colList.map(t => (t._1, t._1.id)).toMap
//            val colListSorted = colList.sortBy(_._2).toIndexedSeq
//            val headersOrdered = if (!endTimestampExclusive.isDefined)
//              colListSorted.map(t => headerMap(t._1))
//            else
//              colListSorted.flatMap(t => Seq(headerMap(t._1) + "_union", headerMap(t._1)))
//            val tableVersion = if (!endTimestampExclusive.isDefined)
//              colListSorted.map(_._1.versionAt(beginTimestamp))
//            else
//              colListSorted.flatMap(t => Seq(t._1.versionUnion(beginTimestamp, endTimestampExclusive.get), t._1.versionAt(beginTimestamp)))
//            if (tableVersion.exists(_.values.size != 0))
//              ColumnVersion.serializeToTable(tableVersion, headersOrdered, new File(resultDir + s"/${tID}_$pID.csv"))
//          }
//      }
//  }
}
