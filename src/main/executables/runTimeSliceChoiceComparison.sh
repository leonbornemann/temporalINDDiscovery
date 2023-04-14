seeds=( -4984250756083210152 8201672769755439997 946021207972520316 -5163658404114984934 7568392567263826981 1492389985696290152 -1735980624967862842 7381495396989348212 -4365538794565510030 -2083048722317635152)
mkdir manyLogsNew
for i in "${!seeds[@]}";
do
  seed=${seeds[i]}
  inputPath=$basePathInput/$bucket/
  outputPath=basePathResults/$bucket
  echo "Running seed $seed "
  java -ea -Xmx240g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.discovery.DiscoveryMain /san2/data/change-exploration/temporalIND/columnHistories/sampleQueries.jsonl /san2/data/change-exploration/temporalIND/discovery/ /san2/data/change-exploration/temporalIND/columnHistories/binaries/4-20000.binary 0.001485149 90 RANDOM 4096 true 10000 $seed
java -ea -Xmx240g -cp code/temporalINDDiscovery/target/scala-2.13/INDInTemporalData-assembly-0.1.jar de.hpi.temporal_ind.discovery.DiscoveryMain /san2/data/change-exploration/temporalIND/columnHistories/sampleQueries.jsonl /san2/data/change-exploration/temporalIND/discovery/ /san2/data/change-exploration/temporalIND/columnHistories/binaries/4-20000.binary 0.001485149 90 WEIGHTED_RANDOM 4096 true 10000 $seed
done