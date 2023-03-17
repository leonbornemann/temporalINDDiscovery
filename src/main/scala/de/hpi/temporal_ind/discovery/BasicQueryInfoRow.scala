package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.time.Instant

case class BasicQueryInfoRow(queryNumber:Int,
                             queryLink: String,
                             queryPageID:String,
                             queryTableID:String,
                             queryColumnID:String) extends JsonWritable[BasicQueryInfoRow]{

  def toCSVLine = {
    s"$queryNumber,$queryLink,$queryPageID,$queryTableID,$queryColumnID"
  }

}
object BasicQueryInfoRow extends JsonReadable[BasicQueryInfoRow]{
  def schema = "queryNumber,queryLink,queryPageID,queryTableID,queryColumnID"

}


