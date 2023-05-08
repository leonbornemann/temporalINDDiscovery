package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.attribute_history.data.ColumnHistoryID
import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class INDCandidateIDs(lhsPageID:String,
                           lhsTableID:String,
                           lhsColumnID:String,
                           rhsPageID:String,
                           rhsTableID:String,
                           rhsColumnID:String) extends JsonWritable[INDCandidateIDs]{
  def lhs = ColumnHistoryID(lhsPageID,lhsTableID,lhsColumnID)


}
object INDCandidateIDs extends JsonReadable[INDCandidateIDs]
