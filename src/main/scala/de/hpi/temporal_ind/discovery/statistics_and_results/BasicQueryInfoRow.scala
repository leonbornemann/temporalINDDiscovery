package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class BasicQueryInfoRow(queryNumber:Int,
                             queryFileName:String,
                             queryLink: String,
                             queryPageID:String,
                             queryTableID:String,
                             queryColumnID:String) extends JsonWritable[BasicQueryInfoRow]{

  def toCSVLine = {
    s"$queryNumber,$queryFileName,$queryLink,$queryPageID,$queryTableID,$queryColumnID"
  }

}
object BasicQueryInfoRow extends JsonReadable[BasicQueryInfoRow]{
  def schema = "queryNumber,queryFileName,queryLink,queryPageID,queryTableID,queryColumnID"

}


