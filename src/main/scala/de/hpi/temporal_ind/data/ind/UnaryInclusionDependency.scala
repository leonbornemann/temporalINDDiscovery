package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.column.ColumnHistory

case class UnaryInclusionDependency(lhsID:String, rhsID:String) {

}
object UnaryInclusionDependency{

  def holdsOn(ch1:ColumnHistory,ch2:ColumnHistory):Boolean = {
    //ch1
    ???
  }
}
