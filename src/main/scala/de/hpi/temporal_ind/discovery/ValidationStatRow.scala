package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

case class ValidationStatRow(queryNumber:Int,
                             queryLink: String,
                             queryPageID:String,
                             queryTableID:String,
                             queryColumnID:String,
                             validationTimeMS: Double,
                             inputSize: Int,
                             truePositiveCount: Int,
                             discoveryVersion:String) extends JsonWritable[ValidationStatRow]{

  def toCSVLine = {
    s"$queryNumber,$queryLink,$queryPageID,$queryTableID,$queryColumnID,$validationTimeMS,$inputSize,$truePositiveCount,$discoveryVersion"
  }

}
object ValidationStatRow extends JsonReadable[ValidationStatRow]{
  def schema = "queryNumber,queryLink,queryPageID,queryTableID,queryColumnID,validationTimeMS,inputSize,truePositiveCount,discoveryVersion"

}


