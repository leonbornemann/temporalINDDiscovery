inputFile="/san2/data/change-exploration/temporalIND/discoveredByMANY/wikipedia/2017-11-05T03\:57\:33Z.txt"
columnHistoryDir="/san2/data/change-exploration/temporalIND/columnHistories/wikipediaFilteredNew/"
outputFileToLabel="/san2/data/change-exploration/temporalIND/labellingOutput/linksResolved.csv"
outputFileStatistics="/san2/data/change-exploration/temporalIND/labellingOutput/linksResolvedStats.csv"
java -ea -Xmx200g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.data.column.labelling.TINDCandidateExportForLabelling wikipedia $inputFile $columnHistoryDir $outputFileToLabel $outputFileStatistics
