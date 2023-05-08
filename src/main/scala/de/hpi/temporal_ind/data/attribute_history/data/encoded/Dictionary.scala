package de.hpi.temporal_ind.data.attribute_history.data.encoded

import de.hpi.temporal_ind.data.{JsonReadable, JsonWritable}

case class Dictionary(allValues: collection.Map[String,Long]) extends JsonWritable[Dictionary]{

}
object Dictionary extends JsonReadable[Dictionary]
