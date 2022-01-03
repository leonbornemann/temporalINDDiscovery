package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.ColumnHistory
import de.hpi.temporal_ind.data.column.io.ColumnHistoryBuffer

import java.io.File

object MANYInputINDValidator extends App {
  val manyInputFIle = new File(args(0))
  val columnHistoryDir = new File(args(1))
  val outputDir = new File(args(2))
  val inds = StaticInclusionDependency.readFromMANYOutputFile(manyInputFIle)
  //filter into buckets according to pageID
  val byPageIDCombination: Map[(String, String), IndexedSeq[StaticInclusionDependency]] = inds.groupBy(ind => (ind.lhsPageID, ind.rhsPageID))
  val rangeToFileIndex = ColumnHistory.getIndexForFilesInDir(columnHistoryDir)
  val buffer = new ColumnHistoryBuffer()
  inds
    .take(100)
    .foreach(ind => {
      val fileLHS = rangeToFileIndex.getFileForPage(BigInt(ind.lhsPageID))
      val fileRHS = rangeToFileIndex.getFileForPage(BigInt(ind.rhsPageID))
      val ch1 = buffer.getOrLoadHistory(fileLHS)(ind.lhsColumnID)
      val ch2 = buffer.getOrLoadHistory(fileRHS)(ind.rhsColumnID)
      val temporalInd = new StrictTemporalIND(ch1, ch2)
      val variant1 = new Variant1TemporalIND(ch1,ch2,30)
      val variant3 = new Variant3TemporalIND(ch1,ch2,30)
      val isValid = temporalInd.isValid
      println(temporalInd.getTabularEventLineageString)
      println(variant1.getTabularEventLineageString)
      println(variant3.getTabularEventLineageString)
      println()
  })

//  byPageIDCombination
//    .foreach{case (lhsPageID,rhsPageID) => {
//
//    }}

}
