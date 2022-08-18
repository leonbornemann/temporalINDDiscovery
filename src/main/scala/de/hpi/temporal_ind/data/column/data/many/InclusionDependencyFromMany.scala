package de.hpi.temporal_ind.data.column.data.many

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.column.data.original.INDCandidate

import java.io.File
import scala.io.Source

case class InclusionDependencyFromMany(lhsID:String, rhsID:String) {

  def toCandidate(indexed: IndexedColumnHistories,filterByUnion:Boolean=false) = {
    if(filterByUnion){
      val lhsColID = lhsColumnID.replace("_union","")
      val rhsColID = lhsColumnID.replace("_union","")
      INDCandidate[String](indexed.multiLevelIndex(lhsPageID)(lhsColID).asOrderedHistory,indexed.multiLevelIndex(rhsPageID)(rhsColID).asOrderedHistory)
    } else {
      INDCandidate[String](indexed.multiLevelIndex(lhsPageID)(lhsColumnID).asOrderedHistory,indexed.multiLevelIndex(rhsPageID)(rhsColumnID).asOrderedHistory)
    }
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
        println(s"processing $s")
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
