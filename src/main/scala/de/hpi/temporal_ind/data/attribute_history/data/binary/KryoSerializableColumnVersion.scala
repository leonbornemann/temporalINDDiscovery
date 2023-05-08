package de.hpi.temporal_ind.data.attribute_history.data.binary

import java.util

@SerialVersionUID(2L)
class KryoSerializableColumnVersion() {

  var revisionID: String = ""
  var revisionDate: String = ""
  var values: java.util.Set[String] = new util.HashSet[String]()
  var header: String = null
  var position: Int = -1
  var columnNotPresent: Boolean = false

}
