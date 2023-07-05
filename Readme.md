# Temporal IND Discovery
The code in this repository is the implementation for the VLDB 2024 Submission "Efficient Discovery of Temporal Inclusion Dependencies".
There are three executable Main-classes to use for the discovery of temporal INDs (tINDs):
* [TINDSearchMain](src/main/scala/de/hpi/temporal_ind/discovery/TINDSearchMain.scala) - Expects input data and queries as well as parameters for indexing and querying. Will then build an index and run all queries against the index, measuring the time of every query.
* [TINDAllPairsMain](src/main/scala/de/hpi/temporal_ind/discovery/TINDAllPairsMain.scala) - Similar to the prior class, but does not expect a set of queries and will instead run in all-pair mode, meaning every input attribute will be queried (in parallel) against the index, discovering all valid tINDs in the input dataset.
* [TINDRuntimeExperiments](src/main/scala/de/hpi/temporal_ind/discovery/TINDRuntimeExperiments.scala) - Convenience class that can run many different parameter settings sequentially. We used this to measure the runtime of different parameter settings. The results should be equivalent to running TINDSearchMain with the same parameters.

## Parameters
Here is the list of most parameters that the above classes expect. For a detailed explanation of most parameters we refer to our paper. Check the code in the above mentioned main classes to see which parameters they expect.
Note that TINDRuntimeExperiments accepts comma-separated lists for many of these paramters and will then run all combinations of parameters. Note that some parameter settings require a rebuild of the index to test and can thus be expected to take significantly longer. Details are in the code.
* __--input__ the input dataset in binary format (see [this repository](https://github.com/HPI-Information-Systems/tindResources) for download links) 
* __--n__ limits the number of attributes taken from input the input dataset, if unspecified, the entire input will be used
* __--result__ the directory in which to store the results
* __--queries__ a list of ids contained in the input set that you wish to query against the index
* __--epsilonQueries__ For Queries: maximum absolute violation that is tolerated (the base time unit that we use is nanoseconds, so to allow a violation of one day with a constant w, epsilon needs to be set to 8.64×10¹³)
* __--deltaQueries__ maximum time shift (in nanoseconds) - TODO!
* __--epsilonIndex__ maximum epsilon that the index expects the queries to use 
* __--deltaQueries__ maximum delta that the index expects the queries to use (Note: deltaQueries can not be set higher than this!)
* * __--w__ weighting function, supported values: {CONSTANT,EXP_DECAY}
* __--timeSliceChoice__ method for choosing time slices, supported values: {WEIGHTED_RANDOM,RANDOM}
* __--m__ size of the bloom filters
* __--k__ number of time slice indices
* __--reverse__ if the flag is set, then queries will be run in reverse search, meaning the index is searched for attributes that are contained in the query
* __--nThreads__ number of threads to use
* __--seed__ seed to make random choices deterministic and experiments repeatable



Furthermore, there are some main classes that produce statistics that we either directly reported in the paper or used to generate plots using our corresponding [Jupyter Notebooks](https://github.com/leonbornemann/temporalINDEvaluation):
* [BasicStatisticsPrintMain](src/main/scala/de/hpi/temporal_ind/discovery/statistics_and_results/BasicStatisticsPrintMain.scala) - produces basic statistics for Section 5.1
* [TINDCandidateExportMain](src/main/scala/de/hpi/temporal_ind/data/attribute_history/labelling/TINDCandidateExportMain.scala) - exports an informative summary of a tIND candidate in csv for manual annotation of genuineness
* [GenuineTINDGridSearchMain](src/main/scala/de/hpi/temporal_ind/data/attribute_history/labelling/GenuineTINDGridSearchMain.scala) - takes the output csv of the previous (with the genuineness column filled) and executes a grid search that tests all tIND variants with various parameters and exports the results to a csv to be processed by our [Jupyter Notebook](TODO) 

