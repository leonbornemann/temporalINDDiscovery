package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.ColumnHistory

import java.io.File
import scala.io.Source

case class StaticInclusionDependency(lhsID:String, rhsID:String) {

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
object StaticInclusionDependency{

  def holdsOn(ch1:ColumnHistory,ch2:ColumnHistory):Boolean = {
    //ch1
    ???
  }

  def readFromMANYOutputFile(file :File):IndexedSeq[StaticInclusionDependency] = {
    Source.fromFile(file).getLines()
      .toIndexedSeq
      .map(s => {
        val endIndex = s.indexOf("][=[")
        val lhs = s.substring(1,endIndex)
        val rhs = s.substring(endIndex+4,s.size-1)
        StaticInclusionDependency(lhs,rhs)
      })
  }
}
