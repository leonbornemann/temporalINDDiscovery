package de.hpi.temporal_ind.data.column.data.original

import java.util

@SerialVersionUID(1L)
class KryoSerializableColumnHistory() {
  var id = ""
  var tableId = ""
  var pageID = ""
  var pageTitle = ""
  var hist:util.List[KryoSerializableColumnVersion] = new util.ArrayList[KryoSerializableColumnVersion]()
}
