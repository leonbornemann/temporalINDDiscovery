basePathInput="/san2/data/change-exploration/temporalIND/exportedForMANY/wikipedia/bucketed"
basePathResults="/san2/data/change-exploration/temporalIND/discoveredByMANY/wikipedia/bucketed/"
mkdir basePathResults
buckets=( "32_20000" "16_32" "8_16"  "4_8" "2_4" "1_2")
mkdir manyLogsNew
for i in "${!buckets[@]}";
do
  bucket=${buckets[i]}
  inputPath=$basePathInput/$bucket/
  outputPath=basePathResults/$bucket
  echo "Running $bucket with input $inputPath and output $outputPath"
  java -ea -Xmx230g -cp jars/MANY-1.2-SNAPSHOT.jar de.metanome.algorithms.many.driver.AnelosimusDriver -inputFileEnding .csv -inputFolderPath "$inputPath/" -filterNonUniqueRefs false -outputFile "$outputPath.txt" -hasHeader true -filterNumericAndShortCols true -filterNullCols true -dop 25  > manyLogsNew/${bucket}.log 2>&1
  #shuffle INDs so that we can randomly sample by just taking first:
  shuf "$outputPath.txt" -o "$outputPath.txt"
done