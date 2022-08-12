bash pullAndCompileTemporalINDCode.sh
timestamp="2017-11-05T03:57:33Z"
manyExportRootDir="/san2/data/change-exploration/temporalIND/exportedForMANY/wikipedia/"
rm "$manyExportRootDir/$timestamp/" -r
echo "Running ExportForManyMain"
java -ea -Xmx32g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.data.column.ExportForManyMain wikipedia /san2/data/change-exploration/temporalIND/columnHistories/wikipediaNew/ /san2/data/change-exploration/temporalIND/columnHistories/wikipediaFilteredNew/ $manyExportRootDir $timestamp
echo "Finished ExportForManyMain - Begin Running MANY"
basePathInput="/san2/data/change-exploration/temporalIND/exportedForMANY/wikipedia/"
basePathResults="/san2/data/change-exploration/temporalIND/discoveredByMANY/wikipedia/"
#snapshotTimes=( "2004-07-26T12:55:44Z" "2007-11-19T12:55:44Z" "2011-07-26T12:55:44Z"  "2015-07-26T12:55:44Z")
snapshotTimes=( "2017-11-05T03:57:33Z")
for i in "${!snapshotTimes[@]}";
do
   currentInstant=${snapshotTimes[i]}
   echo "Running $currentInstant"
   java -ea -Xmx190g -cp jars/MANY-1.2-SNAPSHOT.jar de.metanome.algorithms.many.driver.AnelosimusDriver -inputFileEnding .csv -inputFolderPath "$basePathInput/$currentInstant/" -filterNonUniqueRefs true -outputFile "$basePathResults/${currentInstant}_allowNonUniquePK.txt" -hasHeader true -filterNumericAndShortCols true -filterNullCols true -dop 25  > logs/manyExecutionLogs/${currentInstant}.log 2>&1
   shuf $basePathResults/${currentInstant}.txt -o $basePathResults/${currentInstant}.txt
done
