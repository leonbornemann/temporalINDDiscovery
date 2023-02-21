package de.hpi.temporal_ind.data.ind

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class INDCandidateIDs(lhsPageID:String,
                           lhsTableID:String,
                           lhsColumnID:String,
                           rhsPageID:String,
                           rhsTableID:String,
                           rhsColumnID:String) extends JsonWritable[INDCandidateIDs]{


}
object INDCandidateIDs extends JsonReadable[INDCandidateIDs]
