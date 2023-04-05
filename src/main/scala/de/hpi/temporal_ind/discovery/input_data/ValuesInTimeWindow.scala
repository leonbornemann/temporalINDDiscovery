package de.hpi.temporal_ind.discovery.input_data

import java.time.Instant

class ValuesInTimeWindow(val beginInclusive:Instant,
                         val endExclusive:Instant,
                         val values:collection.Set[String]) {

}
