tablePath="/san2/data/change-exploration/temporalIND/originalTableData/wikipedia/tablesExtracted/matchedWikitableHistories_new/"
columnHistoryDir="/san2/data/change-exploration/temporalIND/columnHistories/wikipediaNew/"
mkdir parallelLogs
mkdir logs
find $tablePath -type f | parallel --delay 1 --eta -j20 -v --joblog parallelLogs/wikipediaTemporalColumnExtraction "java -ea -Xmx16g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.data.wikipedia.ColumnHistoryExtractMain wikipedia {} $columnHistoryDir > logs/{/.}.log 2>&1"

java -ea -Xmx32g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.data.column.ExportForManyMain wikipedia /san2/data/change-exploration/temporalIND/columnHistories/wikipediaNew/ /san2/data/change-exploration/temporalIND/columnHistories/wikipediaFilteredNew/ /san2/data/change-exploration/temporalIND/exportedForMANY/wikipedia/ 2017-11-05T03:57:33Z