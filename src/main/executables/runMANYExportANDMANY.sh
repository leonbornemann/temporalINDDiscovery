bash pullAndCompileTemporalINDCode.sh
timestamp="2017-11-05T03:57:33Z"
manyExportRootDir="/san2/data/change-exploration/temporalIND/exportedForMANY/wikipedia/bucketed/"
echo "Running ExportForManyMain"
java -ea -Xmx32g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.data.column.ExportForManyMain wikipedia /san2/data/change-exploration/temporalIND/columnHistories/buckets_filtered/ $manyExportRootDir $timestamp
echo "Finished ExportForManyMain - Begin Running MANY"

