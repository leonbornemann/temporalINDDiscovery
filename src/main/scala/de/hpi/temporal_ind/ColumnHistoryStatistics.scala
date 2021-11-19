package de.hpi.temporal_ind

import java.time.LocalDateTime

case class ColumnHistoryStatistics(filename:String,
                                   tableID:String,
                                   columnID:String,
                                   humanReadableColumnName:String,
                                   versionTimestamps:List[LocalDateTime],
                                   percentageOfLifetimeNumeric:Double,
                                   percentageOfLifetimeHasHTMLHeaderInContent:Double,
                                  ) //
