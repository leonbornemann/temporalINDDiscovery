package de.hpi.temporal_ind.oneshot

import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

object TimePrintMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  println(GLOBAL_CONFIG.totalTimeInDays - 0.998515*GLOBAL_CONFIG.totalTimeInDays)

}
