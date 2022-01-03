package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

import scala.collection.mutable

case class Dictionary(allValues: collection.Map[String,Long]) extends JsonWritable[Dictionary]{

}
object Dictionary extends JsonReadable[Dictionary]
