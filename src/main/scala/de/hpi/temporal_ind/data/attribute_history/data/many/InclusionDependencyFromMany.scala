package de.hpi.temporal_ind.data.attribute_history.data.many

import de.hpi.temporal_ind.data.attribute_history.data.file_search.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.ind.INDCandidate

import java.io.File
import scala.io.Source

case class InclusionDependencyFromMany(lhsID:String, rhsID:String) {

  def toCandidate(indexed: IndexedColumnHistories,filterByUnion:Boolean=false) = {
    if(filterByUnion){
      val lhsColID = lhsColumnID.replace("_union","")
      val rhsColID = rhsColumnID.replace("_union","")
      INDCandidate[String](indexed.multiLevelIndex(lhsPageID)(lhsColID).asOrderedHistory,indexed.multiLevelIndex(rhsPageID)(rhsColID).asOrderedHistory)
    } else {
      INDCandidate[String](indexed.multiLevelIndex(lhsPageID)(lhsColumnID).asOrderedHistory,indexed.multiLevelIndex(rhsPageID)(rhsColumnID).asOrderedHistory)
    }
  }

  def toMANYString = {
    //[340203519-5_25959604.csv.0a845016-f912-4e18-8922-1f246f407452][=[367621044-0_27613671.csv.e0aec3f0-5c8c-44f1-b955-bda304e0c206]
    s"[$lhsID][=[$rhsID]"
  }

  def toCandidateWithIncrementalIndex(index: IncrementalIndexedColumnHistories, filterByUnion: Boolean = false) = {
    val lhsColID = if(filterByUnion) lhsColumnID.replace("_union", "") else lhsColumnID
    val rhsColID = if(filterByUnion) rhsColumnID.replace("_union", "") else rhsColumnID
    INDCandidate[String](index.getOrLoad(lhsPageID.toLong,lhsColID).asOrderedHistory, index.getOrLoad(rhsPageID.toLong,rhsColID).asOrderedHistory)
  }

  private def getColumnID(str: String) = {
    val tokens = str.split("\\.")
    tokens(tokens.size-1)
  }

  def lhsColumnID = getColumnID(lhsID)
  def rhsColumnID = getColumnID(rhsID)
  def antecedentTableID = getTableID(lhsID)
  def dependentTableID = getTableID(rhsID)
  def lhsPageID = getPageID(lhsID)
  def rhsPageID = getPageID(rhsID)

  private def getTableID(str: String) = {
      str.split("\\.csv")(0).split("_")(0)
  }

  private def getPageID(str: String) = {
    str.split("\\.csv")(0).split("_")(1)
  }

}
object InclusionDependencyFromMany{

  def readFromMANYOutputFile(file :File):Iterator[InclusionDependencyFromMany] = {
    Source.fromFile(file).getLines()
      .map(s => {
        fromManyOutputString(s)
      })
  }

  def fromManyOutputString(s: String) = {
    val endIndex = s.indexOf("][=[")
    val lhs = s.substring(1, endIndex)
    val rhs = s.substring(endIndex + 4, s.size - 1)
    InclusionDependencyFromMany(lhs, rhs)
  }
}
